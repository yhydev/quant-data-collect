import unittest
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from events.order_watcher import OrderStatus
from models.interfaces import InitialParams, OrderSequence, TradeResult
import services.batch_service as batch_service_module
from services.batch_service import BatchExecutionService, Phase


class FakeSession:
    def __init__(self):
        self.commit = AsyncMock()


class FakeResult:
    def __init__(self, items):
        self._items = items

    def scalar_one_or_none(self):
        return self._items[0] if self._items else None

    def scalars(self):
        return self

    def all(self):
        return list(self._items)


class FakeDbSession:
    def __init__(self, db):
        self.db = db

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0].get("entity")
        name = getattr(entity, "__name__", "")
        if name == "BatchExecute":
            text = str(stmt)
            if "batch_execute.id" in text:
                return FakeResult([self.db["batch"]])
            if "batch_execute.phase" in text:
                batch = self.db["batch"]
                waiting = (
                    batch.execute_status == "RUNNING"
                    and batch.phase in (Phase.FIRST_ORDER_WAIT, Phase.SECOND_ORDER_WAIT)
                )
                return FakeResult([batch] if waiting else [])
            if "batch_execute.position_execute_id" in text:
                return FakeResult([self.db["batch"]])
            return FakeResult([self.db["batch"]])
        if name == "PositionExecute":
            return FakeResult([self.db["position"]])
        return FakeResult([])

    async def commit(self):
        return None


class OpeningFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        batch_service_module.logger = MagicMock()
        self.service = BatchExecutionService()

        self.service.collector = MagicMock()
        self.service.order_plugin = MagicMock()
        self.service.trader = MagicMock()

        self.service._check_position_complete = AsyncMock()

        self.batch = SimpleNamespace(
            id=1,
            position_execute_id=10,
            batch_value=1000.0,
            timeout=300,
            order_sequence=None,
            contract_price=None,
            spot_price=None,
            phase=Phase.PENDING,
            execute_status="RUNNING",
            first_side_order_id=None,
            first_side_filled_price=None,
            second_side_order_id=None,
            second_side_filled_price=None,
            complete_reason=None,
            position=SimpleNamespace(contract="BTCUSDT"),
        )
        self.session = FakeSession()

    async def test_opening_happy_path_with_mocked_external_services(self):
        self.service.order_plugin.get_initial_params = AsyncMock(
            return_value=InitialParams(
                order_sequence=OrderSequence.FUTURES_FIRST,
                contract_price=62000.0,
                spot_price=61950.0,
            )
        )
        self.service.trader.open_futures_short = AsyncMock(
            return_value=TradeResult(success=True, order_id=10001)
        )
        self.service.trader.buy_spot = AsyncMock(
            return_value=TradeResult(success=True, order_id=20002)
        )
        self.service.trader.get_order_status = AsyncMock(
            return_value={"status": "FILLED", "avgPrice": "61950.0"}
        )
        self.service.trader.transfer_to_savings = AsyncMock(
            return_value=TradeResult(success=True, message="ok")
        )

        await self.service._execute_current_phase(self.batch, self.session)
        self.assertEqual(self.batch.phase, Phase.FIRST_ORDER_OPEN)
        self.assertEqual(self.batch.order_sequence, "futures_first")

        await self.service._execute_current_phase(self.batch, self.session)
        self.assertEqual(self.batch.phase, Phase.FIRST_ORDER_WAIT)
        self.assertEqual(self.batch.first_side_order_id, "10001")
        self.service.trader.open_futures_short.assert_awaited_once_with(
            "BTCUSDT", 1000.0, 62000.0
        )

        await self.service._handle_filled(
            self.batch,
            phase=Phase.FIRST_ORDER_WAIT,
            filled_price=62000.0,
            is_spot=False,
            session=self.session,
        )
        self.assertEqual(self.batch.phase, Phase.FIRST_FILLED)

        await self.service._execute_current_phase(self.batch, self.session)
        self.assertEqual(self.batch.phase, Phase.SECOND_ORDER_OPEN)

        await self.service._execute_current_phase(self.batch, self.session)
        self.assertEqual(self.batch.phase, Phase.SECOND_ORDER_WAIT)
        self.assertEqual(self.batch.second_side_order_id, "20002")
        self.service.trader.buy_spot.assert_awaited_once_with(
            "BTCUSDT", 1000.0, 61950.0
        )

        await self.service._handle_filled(
            self.batch,
            phase=Phase.SECOND_ORDER_WAIT,
            filled_price=61950.0,
            is_spot=True,
            session=self.session,
        )

        self.assertEqual(self.batch.phase, Phase.COMPLETED)
        self.assertEqual(self.batch.execute_status, "COMPLETED")
        self.assertEqual(self.batch.complete_reason, "SUCCESS")
        self.service.trader.transfer_to_savings.assert_awaited_once()

    async def test_first_order_open_failure_raises(self):
        self.batch.order_sequence = "futures_first"
        self.batch.contract_price = 62000.0
        self.batch.phase = Phase.FIRST_ORDER_OPEN
        self.service.trader.open_futures_short = AsyncMock(
            return_value=TradeResult(success=False, message="rejected")
        )

        with self.assertRaises(Exception):
            await self.service._execute_current_phase(self.batch, self.session)

        self.assertEqual(self.batch.phase, Phase.FIRST_ORDER_OPEN)

    async def test_cancelled_order_rolls_back_phase(self):
        self.batch.phase = Phase.SECOND_ORDER_WAIT
        await self.service._handle_cancelled(self.batch, self.batch.phase, self.session)
        self.assertEqual(self.batch.phase, Phase.FIRST_FILLED)

    async def test_orchestrated_opening_flow_with_polling_and_events(self):
        position = SimpleNamespace(
            id=10,
            contract="BTCUSDT",
            execute_status="RUNNING",
            complete_reason=None,
            updated_at=None,
        )
        batch = SimpleNamespace(
            id=1,
            position_execute_id=10,
            batch_value=1000.0,
            timeout=300,
            execute_status="RUNNING",
            complete_reason=None,
            phase=Phase.PENDING,
            order_sequence=None,
            contract_price=None,
            spot_price=None,
            first_side_order_id=None,
            second_side_order_id=None,
            first_side_filled_price=None,
            second_side_filled_price=None,
            position=position,
            updated_at=datetime.utcnow(),
        )
        db = {"batch": batch, "position": position}

        @asynccontextmanager
        async def fake_get_async_session():
            yield FakeDbSession(db)

        service = BatchExecutionService()
        service.collector = MagicMock()
        service.order_plugin = MagicMock()
        service.trader = MagicMock()

        service.order_plugin.get_initial_params = AsyncMock(
            return_value=InitialParams(
                order_sequence=OrderSequence.FUTURES_FIRST,
                contract_price=62010.0,
                spot_price=61990.0,
            )
        )
        service.trader.open_futures_short = AsyncMock(
            return_value=TradeResult(success=True, order_id=111)
        )
        service.trader.buy_spot = AsyncMock(
            return_value=TradeResult(success=True, order_id=222)
        )
        service.trader.transfer_to_savings = AsyncMock(
            return_value=TradeResult(success=True)
        )
        service.trader.get_order_status = AsyncMock(
            side_effect=[
                {"status": "FILLED", "avgPrice": "62010.0"},
                {"status": "FILLED", "avgPrice": "61990.0"},
            ]
        )

        with patch.object(batch_service_module, "get_async_session", fake_get_async_session):
            await service.execute_batch(batch.id)
            self.assertEqual(batch.phase, Phase.FIRST_ORDER_OPEN)

            await service.execute_batch(batch.id)
            self.assertEqual(batch.phase, Phase.FIRST_ORDER_WAIT)

            await service.poll_order_status()
            self.assertEqual(batch.phase, Phase.FIRST_FILLED)

            await service.execute_batch(batch.id)
            self.assertEqual(batch.phase, Phase.SECOND_ORDER_OPEN)

            await service.execute_batch(batch.id)
            self.assertEqual(batch.phase, Phase.SECOND_ORDER_WAIT)

            await service.poll_order_status()

        self.assertEqual(batch.phase, Phase.COMPLETED)
        self.assertEqual(batch.execute_status, "COMPLETED")
        self.assertEqual(batch.complete_reason, "SUCCESS")
        self.assertEqual(position.execute_status, "COMPLETED")
        self.assertEqual(position.complete_reason, "SUCCESS")
        service.trader.open_futures_short.assert_awaited_once()
        service.trader.buy_spot.assert_awaited_once()
        service.trader.transfer_to_savings.assert_awaited_once()

    async def test_second_order_cancelled_then_retry_to_success(self):
        position = SimpleNamespace(
            id=10,
            contract="BTCUSDT",
            execute_status="RUNNING",
            complete_reason=None,
            updated_at=None,
        )
        batch = SimpleNamespace(
            id=1,
            position_execute_id=10,
            batch_value=1000.0,
            timeout=300,
            execute_status="RUNNING",
            complete_reason=None,
            phase=Phase.SECOND_ORDER_WAIT,
            order_sequence="futures_first",
            contract_price=62010.0,
            spot_price=61990.0,
            first_side_order_id="111",
            second_side_order_id="222",
            first_side_filled_price=62010.0,
            second_side_filled_price=None,
            position=position,
            updated_at=datetime.utcnow(),
        )
        db = {"batch": batch, "position": position}

        @asynccontextmanager
        async def fake_get_async_session():
            yield FakeDbSession(db)

        service = BatchExecutionService()
        service.collector = MagicMock()
        service.order_plugin = MagicMock()
        service.trader = MagicMock()
        service.trader.buy_spot = AsyncMock(
            return_value=TradeResult(success=True, order_id=333)
        )
        service.trader.transfer_to_savings = AsyncMock(return_value=TradeResult(success=True))
        service.trader.get_order_status = AsyncMock(
            side_effect=[
                {"status": "CANCELLED", "avgPrice": "0"},
                {"status": "FILLED", "avgPrice": "61980.0"},
            ]
        )

        with patch.object(batch_service_module, "get_async_session", fake_get_async_session):
            await service.poll_order_status()
            self.assertEqual(batch.phase, Phase.FIRST_FILLED)

            await service.execute_batch(batch.id)
            self.assertEqual(batch.phase, Phase.SECOND_ORDER_OPEN)

            await service.execute_batch(batch.id)
            self.assertEqual(batch.phase, Phase.SECOND_ORDER_WAIT)
            self.assertEqual(batch.second_side_order_id, "333")

            await service.poll_order_status()

        self.assertEqual(batch.phase, Phase.COMPLETED)
        self.assertEqual(batch.complete_reason, "SUCCESS")
        self.assertEqual(position.execute_status, "COMPLETED")
        service.trader.buy_spot.assert_awaited_once_with("BTCUSDT", 1000.0, 61990.0)

    async def test_check_position_complete_prefers_timeout_reason(self):
        position = SimpleNamespace(
            id=88,
            contract="ETHUSDT",
            execute_status="RUNNING",
            complete_reason=None,
            updated_at=None,
        )
        batch = SimpleNamespace(
            id=2,
            position_execute_id=88,
            phase=Phase.COMPLETED,
            complete_reason="TIMEOUT",
        )
        db = {"batch": batch, "position": position}

        @asynccontextmanager
        async def fake_get_async_session():
            yield FakeDbSession(db)

        service = BatchExecutionService()

        with patch.object(batch_service_module, "get_async_session", fake_get_async_session):
            await service._check_position_complete(88)

        self.assertEqual(position.execute_status, "COMPLETED")
        self.assertEqual(position.complete_reason, "TIMEOUT")


if __name__ == "__main__":
    unittest.main()
