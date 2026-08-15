#!/usr/bin/env python3
"""download_funding.py — 全量下载币安 USDT-M 永续合约 funding rate 历史（GitHub Actions / 本地通用）。

数据源：data.binance.vision 官方公开数据站（无鉴权、无地理封锁、无 rate limit）
  https://data.binance.vision/data/futures/um/monthly/fundingRate/{SYMBOL}/{SYMBOL}-fundingRate-{YYYY-MM}.zip
  月度 zip 内 CSV 列: calc_time(ms epoch UTC), funding_interval_hours, last_funding_rate

输出（--out 目录）:
  combined.parquet      全币种合并表: symbol, date(datetime64[ms,UTC]), funding_rate, funding_interval_hours
  per_symbol/{SYM}.parquet 单币种表（同列，无 symbol 列）
  summary.csv           每币统计: symbol, records, first_date, last_date, mean_rate, min/max interval
  manifest.json         元信息（币数、总记录数、起止、生成时间、逐币记录数）
  funding-rate-full-{date}.zip  上述内容的打包（--zip 时）
  fallback（无 pandas 时）: per_symbol/{SYM}.csv + summary.csv + manifest.json

用法示例:
  python3 download_funding.py --symbols BTCUSDT,ETHUSDT --start 2024-01 --end 2026-08 --out out/
  python3 download_funding.py --symbols-file symbols.txt --start 2024-01 --end 2026-08 --workers 16
"""
import argparse, csv, io, json, os, sys, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import urllib.request, urllib.error

BASE = "https://data.binance.vision/data/futures/um/monthly/fundingRate"
UA = {"User-Agent": "quant-data-collect/1.0"}


def month_span(start: str, end: str):
    """'2024-01'..'2026-08' 闭区间月序列"""
    ys, ms = map(int, start.split("-"))
    ye, me = map(int, end.split("-"))
    out = []
    y, m = ys, ms
    while (y, m) <= (ye, me):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def http_get(url: str, retries: int = 6, timeout: int = 60):
    """返回 (status, bytes)；404 直接返回；网络错误重试后抛出"""
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return 404, b""
            last = e
            time.sleep(2 * (i + 1))
        except Exception as e:  # 网络抖动
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"download failed after {retries} retries: {url}: {last}")


FAPI = "https://fapi.binance.com/fapi/v1/fundingRate"


def fetch_month_via_api(sym: str, ym: str, timeout=60):
    """fapi fundingRate API 兜底：拉取某月全部记录（当月 data 站未发布时用）。
    返回 {calc_time_ms: (rate, interval_h)}；网络失败/币种不存在则返回 None/空。"""
    y, m = int(ym[:4]), int(ym[5:7])
    start_ms = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = start_ms + 32 * 86400 * 1000  # 宽松上限（下月初）
    out: dict = {}
    cur = start_ms
    while cur < end_ms:
        url = f"{FAPI}?symbol={sym}&startTime={cur}&limit=1000"
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                rows = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):  # 币种已下架/不存在
                return None
            if e.code in (429, 418):
                time.sleep(60 if e.code == 418 else 10)
                continue
            time.sleep(3)
            continue
        except Exception:
            time.sleep(3)
            continue
        if not rows:
            break
        for r in rows:
            t = int(r["fundingTime"])
            if t < end_ms:
                out[t] = (float(r["fundingRate"]), 0)  # API 无 interval 字段
        cur = rows[-1]["fundingTime"] + 1
        time.sleep(0.35)  # 无 key 限速：weight 5/次，~1200 weight/min → 保守 3 req/s
        if len(rows) < 1000:
            break
    return out


def fetch_symbol(sym: str, months, progress=None, idx=0, total=1,
                 include_current=False):
    """下载单币全历史 funding rate，返回 [(calc_time_ms, rate, interval_h), ...]（按时间升序、去重）。
    去重键 = 对齐到小时（funding 事件必然在整点；data 站与 API 的时间戳可能差 1ms）。"""
    rows: dict = {}
    n_miss = 0
    now = datetime.now(timezone.utc)
    cur_month = now.strftime("%Y-%m")
    for ym in months:
        url = f"{BASE}/{sym}/{sym}-fundingRate-{ym}.zip"
        st, data = http_get(url)
        if st != 200:
            n_miss += 1
            if include_current and ym == cur_month:
                # 当月 data 站未发布 → API 兜底
                api_rows = fetch_month_via_api(sym, ym)
                if api_rows:
                    for t, (fr, iv) in api_rows.items():
                        rows[t // 3600000 * 3600000] = (fr, iv)  # 对齐小时
                    print(f"  [api-fallback] {sym} {ym}: {len(api_rows)} records", flush=True)
            continue
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
                text = z.read(name).decode("utf-8", "replace")
        except Exception as e:
            print(f"  [WARN] {sym} {ym}: bad zip: {e}", flush=True)
            continue
        for r in csv.DictReader(io.StringIO(text)):
            try:
                t = int(r["calc_time"])
                fr = float(r["last_funding_rate"])
                iv = int(float(r.get("funding_interval_hours") or 0))
            except (KeyError, ValueError):
                continue
            rows[t // 3600000 * 3600000] = (fr, iv)  # 同事件去重（小时对齐）
    ts = sorted(rows)
    if progress:
        progress(idx, total, sym, len(ts), n_miss)
    return sym, [(t, rows[t][0], rows[t][1]) for t in ts]


def load_symbols(symbols_arg, symbols_file):
    syms = []
    if symbols_arg:
        syms += [s.strip().upper() for s in symbols_arg.split(",") if s.strip()]
    if symbols_file:
        p = Path(symbols_file)
        if p.exists():
            syms += [l.strip().upper() for l in p.read_text().splitlines()
                     if l.strip() and not l.startswith("#")]
        else:
            print(f"[FATAL] symbols file not found: {p}", file=sys.stderr)
            sys.exit(2)
    # 去重保序
    seen, out = set(), []
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def main():
    ap = argparse.ArgumentParser(description="Binance USDT-M funding rate full-history downloader")
    ap.add_argument("--symbols", help="comma-separated symbols (e.g. BTCUSDT,ETHUSDT)")
    ap.add_argument("--symbols-file", help="file with one symbol per line")
    ap.add_argument("--start", default="2024-01", help="start month YYYY-MM")
    ap.add_argument("--end", default=None, help="end month YYYY-MM (default: current month)")
    ap.add_argument("--out", default="out", help="output dir")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=0, help="仅下载前 N 个币（冒烟用）")
    ap.add_argument("--include-current", action="store_true",
                    help="当月 data 站未发布时用 fapi API 补齐（默认：end=上月则无影响）")
    ap.add_argument("--zip", action="store_true", help="打包 funding-rate-full-<date>.zip")
    args = ap.parse_args()

    if not args.end:
        args.end = datetime.now(timezone.utc).strftime("%Y-%m")
    if args.end >= datetime.now(timezone.utc).strftime("%Y-%m") and not args.include_current:
        # 默认 end=当月但 data 站当月通常滞后 → 自动降级为上月（完整月）
        y, m = map(int, args.end.split("-"))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        args.end = f"{y:04d}-{m:02d}"
        print(f"[auto] end 当月且未开 --include-current → 使用完整月 {args.end}（当月可开 --include-current 走 API 补齐）")
    symbols = load_symbols(args.symbols, args.symbols_file)
    if args.limit > 0:
        symbols = symbols[: args.limit]
    if not symbols:
        print("[FATAL] no symbols given", file=sys.stderr)
        sys.exit(2)

    months = month_span(args.start, args.end)
    print(f"[start] symbols={len(symbols)} months={args.start}..{args.end} "
          f"({len(months)}mo) workers={args.workers}", flush=True)

    t0 = time.time()
    results = {}
    lock_note = {"n": 0}

    def prog(idx, total, sym, n, miss):
        lock_note["n"] += 1
        done = lock_note["n"]
        el = time.time() - t0
        print(f"[{done}/{total}] {sym}: {n} records (miss={miss}) "
              f"{el:.0f}s elapsed, {el/max(done,1)*done:.0f}s/coin est", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch_symbol, s, months, prog, i, len(symbols),
                          args.include_current): s
                for i, s in enumerate(symbols)}
        for f in as_completed(futs):
            sym, recs = f.result()
            results[sym] = recs

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    per_dir = out / "per_symbol"
    per_dir.mkdir(parents=True, exist_ok=True)

    # ------- 元信息 -------
    manifest = {
        "source": BASE,
        "symbols_requested": len(symbols),
        "symbols_with_data": 0,
        "total_records": 0,
        "start_month": args.start,
        "end_month": args.end,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "per_symbol": {},
    }

    try:
        import pandas as pd  # noqa
        HAS_PANDAS = True
    except Exception:
        HAS_PANDAS = False
    print(f"[pandas] {'available (parquet output)' if HAS_PANDAS else 'NOT available (csv fallback)'}")

    summary_rows = []
    all_frames = []
    for sym, recs in sorted(results.items()):
        if not recs:
            manifest["per_symbol"][sym] = {"records": 0}
            continue
        manifest["symbols_with_data"] += 1
        manifest["total_records"] += len(recs)
        ts = [r[0] for r in recs]
        rates = [r[1] for r in recs]
        ivals = [r[2] for r in recs]
        meta = {
            "records": len(recs),
            "first": datetime.fromtimestamp(ts[0] / 1000, timezone.utc).isoformat(),
            "last": datetime.fromtimestamp(ts[-1] / 1000, timezone.utc).isoformat(),
            "mean_rate": round(sum(rates) / len(rates), 9),
            "interval_hours": sorted(set(ivals)) if ivals else [],
        }
        manifest["per_symbol"][sym] = meta
        summary_rows.append([sym, len(recs), meta["first"], meta["last"],
                             meta["mean_rate"], ",".join(map(str, meta["interval_hours"]))])

        if HAS_PANDAS:
            df = pd.DataFrame({
                "date": pd.to_datetime([t for t in ts], unit="ms", utc=True).astype("datetime64[ms, UTC]"),
                "funding_rate": rates,
                "funding_interval_hours": ivals,
            })
            df.to_parquet(per_dir / f"{sym}.parquet", index=False)
        else:
            with open(per_dir / f"{sym}.csv", "w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["date", "funding_rate", "funding_interval_hours"])
                for t, fr, iv in recs:
                    w.writerow([datetime.fromtimestamp(t / 1000, timezone.utc).isoformat(), fr, iv])
        if HAS_PANDAS:
            df["symbol"] = sym
            all_frames.append(df)

    # ------- 合并表 -------
    if HAS_PANDAS and all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined = combined[["symbol", "date", "funding_rate", "funding_interval_hours"]]
        combined.to_parquet(out / "combined.parquet", index=False)
        print(f"[combined] {combined.shape[0]} rows -> {out/'combined.parquet'}")

    with open(out / "summary.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "records", "first_date", "last_date", "mean_rate", "interval_hours"])
        w.writerows(summary_rows)

    with open(out / "manifest.json", "w") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)

    # ------- zip -------
    if args.zip:
        zip_name = f"funding-rate-full-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.zip"
        zip_path = out / zip_name
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            for p in sorted(out.glob("combined.parquet")) + sorted(per_dir.glob("*.parquet")) + \
                     sorted(per_dir.glob("*.csv")) + [out / "summary.csv", out / "manifest.json"]:
                if p.exists():
                    z.write(p, p.relative_to(out))
        print(f"[zip] {zip_path} ({zip_path.stat().st_size / 1e6:.1f} MB)")

    el = time.time() - t0
    print(f"[done] {manifest['symbols_with_data']} symbols, {manifest['total_records']} records, "
          f"{el:.0f}s total -> {out}")
    sys.exit(0 if manifest["symbols_with_data"] else 1)


if __name__ == "__main__":
    main()
