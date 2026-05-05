"""Funding rate history sync service."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import select, and_, tuple_

from models.database import FundingRateHistory, get_async_session

logger = logging.getLogger(__name__)


class FundingRateSyncService:
    """Sync all-symbol funding history in rolling window."""

    def __init__(self, collector, days: int = 10, page_limit: int = 1000):
        self.collector = collector
        self.days = days
        self.page_limit = max(1, min(page_limit, 1000))

    async def sync_recent_window(self) -> dict:
        """Fetch full window from exchange and upsert missing rows."""
        now = datetime.utcnow()
        start = now - timedelta(days=self.days)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        cursor_ms = start_ms
        pages = 0
        fetched = 0
        inserted = 0

        while cursor_ms <= end_ms:
            rows = await self.collector.get_funding_rate_history_window(
                start_time_ms=cursor_ms,
                end_time_ms=end_ms,
                limit=self.page_limit,
            )
            pages += 1

            if not rows:
                break

            fetched += len(rows)
            inserted += await self._insert_missing_rows(rows)

            max_funding_time_ms = max(int(item.get("fundingTime", 0) or 0) for item in rows)
            if max_funding_time_ms <= 0:
                break
            if len(rows) < self.page_limit:
                break

            cursor_ms = max_funding_time_ms + 1

        logger.info(
            "Funding history sync done: days=%s pages=%s fetched=%s inserted=%s",
            self.days,
            pages,
            fetched,
            inserted,
        )

        return {
            "days": self.days,
            "pages": pages,
            "fetched": fetched,
            "inserted": inserted,
            "start_time": start.isoformat(),
            "end_time": now.isoformat(),
        }

    async def cleanup_invalid_rows(self) -> int:
        """Delete rows with invalid funding time."""
        async with get_async_session() as session:
            result = await session.execute(
                select(FundingRateHistory).where(FundingRateHistory.next_funding_time <= 0)
            )
            rows = list(result.scalars().all())
            count = len(rows)
            for row in rows:
                await session.delete(row)
            if count > 0:
                await session.commit()
            return count

    async def _insert_missing_rows(self, rows: list[dict]) -> int:
        keys: list[tuple[str, int]] = []
        normalized = []
        for item in rows:
            symbol = str(item.get("symbol", "")).strip()
            funding_time_ms = int(item.get("fundingTime", 0) or 0)
            if not symbol or funding_time_ms <= 0:
                continue
            funding_time_s = funding_time_ms // 1000
            keys.append((symbol, funding_time_s))
            normalized.append((symbol, funding_time_s, item))

        if not normalized:
            return 0

        async with get_async_session() as session:
            result = await session.execute(
                select(FundingRateHistory.symbol, FundingRateHistory.next_funding_time).where(
                    tuple_(FundingRateHistory.symbol, FundingRateHistory.next_funding_time).in_(keys)
                )
            )
            existing = set((row[0], int(row[1])) for row in result.all())

            added = 0
            for symbol, funding_time_s, item in normalized:
                if (symbol, funding_time_s) in existing:
                    continue
                rate = float(item.get("fundingRate", 0) or 0)
                mark_price = float(item.get("markPrice", 0) or 0)
                session.add(
                    FundingRateHistory(
                        symbol=symbol,
                        rate=rate,
                        estimated_rate=mark_price,
                        next_funding_time=funding_time_s,
                        recorded_at=datetime.utcnow(),
                    )
                )
                added += 1

            if added > 0:
                await session.commit()

            return added
