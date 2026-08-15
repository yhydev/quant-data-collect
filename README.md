# Binance USDT-M 永续合约 Funding Rate 全量下载（GitHub Actions + Release）

自动从币安官方公开数据站 **data.binance.vision** 下载全部 USDT-M 永续合约（500+ 币）的
**全历史 funding rate**（2024-01 起，与量价数据 `panel_all_525.parquet` / `gap_oos_525.parquet`
同范围），打包为 parquet/zip 并发布为 **GitHub Release 资产**，提供直接下载链接。

- 数据源无鉴权、无 API key、无地理封锁、无 rate limit（月度 zip 批量拉取）
- 每月 1 日定时自动补新月份；也可手动触发（含冒烟参数）
- 当月数据站未发布时可选走 fapi API 补齐（`include_current=true`）
- 零 secret 要求（Release 由 GITHUB_TOKEN 创建，权限见 workflow）

## 数据输出（每次运行产物）

| 文件 | 说明 |
|---|---|
| `combined.parquet` | 全币种合并表：`symbol, date, funding_rate, funding_interval_hours`（date = datetime64[ms, UTC]） |
| `per_symbol/{SYMBOL}.parquet` | 每币一个表（同列，无 symbol 列） |
| `summary.csv` | 逐币统计：记录数 / 起止 / 均值 / 采样间隔 |
| `manifest.json` | 元信息：币数、总记录数、逐币详情、生成时间 |
| `funding-rate-full-*.zip` | 以上全部打包（约几十 MB） |

> 原始 CSV 字段 `calc_time`(ms epoch UTC), `funding_interval_hours`, `last_funding_rate`。
> 采样间隔随币种/时段变化（8h 为主，部分币 4h 或 1h，以 `funding_interval_hours` 列为准）。
> 注意：币安 funding 事件必然落在整点；脚本按"对齐到小时"去重，避免 data 站与 API 时间戳
> 相差 1ms 导致的重复事件。

## 触发方式

1. **手动**：仓库 Actions → `funding-rate-full-download` → Run workflow，可填：
   - `start_month`：默认 `2024-01`（改为 `2020-01` 可拉更早，data 站有币安上线以来全部历史）
   - `end_month`：留空 = 上一个完整月（推荐）；`current` = 强制含当月
   - `symbol_limit`：冒烟用（如 `5` = 只处理前 5 个币）
   - `symbols_override`：指定币种（逗号分隔）
   - `include_current`：true = 当月走 fapi API 补齐（runner 网络需可达 fapi.binance.com）
   - `create_release`：false = 只跑下载+artifact，不建 Release
2. **定时**：每月 1 日 06:00 UTC 自动运行（结束月=上月完整月，不带当月 API 补齐）

## 本地运行

```bash
pip install -r requirements.txt
python3 download_funding.py --symbols-file symbols.txt --start 2024-01 --end 2026-07 --workers 16 --out out --zip
# 当月补齐：
python3 download_funding.py --symbols-file symbols.txt --start 2024-01 --end 2026-08 --include-current --out out --zip
```

## 与量价数据对齐（重要）

排查结论（详见本 repo 根因分析部分 / 任务报告）：

1. **币种符号格式不同**：量价 parquet 里是 `BTCUSDT`（无下划线），144 上的旧 funding
   feather 文件名是 `BTC_USDT-1h-futures-funding-rate.feather`（下划线）。直接按 symbol 字符串
   join 是 **0 命中**。本 repo 统一输出 `BTCUSDT`（与量价一致）。
2. **时间范围不同**：`full_fut_all.parquet` 只覆盖 2026-02-13 起（6 个月）；2024 起的量价在
   `panel_all_525.parquet` / `gap_oos_525.parquet`。旧的 `dl_funding_fut.py` 硬编码起点
   2026-02-01，只有 6.5 个月，自然对不上。本 workflow 默认 2024-01 起全历史。
3. **采样频率**：funding 实际 8h/4h/1h 随币种变化（文件名里的 `1h` 只是 freqtrade 命名约定，
   不是真实频率）。与 1h K 线对齐时：
   ```python
   import pandas as pd
   fr = pd.read_parquet("combined.parquet")          # symbol/date/funding_rate/...
   k  = pd.read_parquet("panel_all_525.parquet")     # symbol/open_time/...
   k["date"] = pd.to_datetime(k["open_time"], unit="ms", utc=True)
   # funding 事件落在整点，floor 到小时后与 K 线 open_time merge
   fr["date"] = fr["date"].dt.floor("h")
   merged = k.merge(fr, on=["symbol", "date"], how="left")  # 未对齐的 bar 为 NaN，可用 ffill
   ```
4. **时间戳毫秒偏移**：fapi 的 `fundingTime` 偶带 `...001` ms 尾数，直接等值匹配会丢一半
   记录；floor 到小时即可 100% 命中 1h bar。

## Release 下载

每次全量运行自动创建 Release：`funding-rate-YYYY-MM-DD-HHMM`（保留最近 3 个），
资产含 zip / combined.parquet / summary.csv / manifest.json，见仓库 Releases 页。
