"""Service to create and cache phase rule executer."""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

from .rule_executer import RuleExecuter
from plugins.order_sequence import get_plugin


logger = logging.getLogger(__name__)


class RuleExecuterService:
    """Factory/cache service for phase rule executer."""

    def __init__(self) -> None:
        self._executer: RuleExecuter | None = None
        self._actions: dict[str, Any] = {
            "initialize_params": self._initialize_params,
            "open_first_order": self._open_first_order,
            "open_second_order": self._open_second_order,
            "to_second_order_open": self._to_second_order_open,
            "noop": self._noop,
        }

    def get_or_create(self) -> RuleExecuter:
        """Return cached executer or create a new one."""
        if self._executer is None:
            self._executer = RuleExecuter(
                self._build_config(),
                self._actions,
                post_action=self._post_action,
            )
        return self._executer

    async def execute(self, batch, session, batch_service) -> dict[str, Any] | None:
        """Execute current phase rules using internal actions."""
        batch._rule_ctx = {
            "session": session,
            "batch_service": batch_service,
        }
        try:
            executer = self.get_or_create()
            return await executer.execute(batch)
        finally:
            if hasattr(batch, "_rule_ctx"):
                delattr(batch, "_rule_ctx")

    def _build_config(self) -> dict[str, Any]:
        return {
            "nodeField": "phase",
            "rules": [
                {
                    "condition": 'phase == "PENDING"',
                    "action": "initialize_params",
                },
                {
                    "condition": 'phase == "FIRST_ORDER_OPEN"',
                    "action": "open_first_order",
                },
                {
                    "condition": 'phase == "FIRST_ORDER_WAIT"',
                    "action": "noop",
                },
                {
                    "condition": 'phase == "FIRST_FILLED"',
                    "action": "to_second_order_open",
                },
                {
                    "condition": 'phase == "SECOND_ORDER_OPEN"',
                    "action": "open_second_order",
                },
                {
                    "condition": 'phase == "SECOND_ORDER_WAIT"',
                    "action": "noop",
                },
                {
                    "condition": 'phase == "COMPLETED"',
                    "action": "noop",
                },
            ],
        }

    async def _initialize_params(self, batch):
        ctx = self._get_ctx(batch)
        service = ctx["batch_service"]

        plugin = service.order_plugin
        requested_plugin_name = getattr(batch, "order_sequence", None)
        if requested_plugin_name:
            try:
                plugin = get_plugin(requested_plugin_name)
            except ValueError:
                logger.warning(
                    "Batch %s: unknown plugin %s, fallback to default",
                    batch.id,
                    requested_plugin_name,
                )

        params = await plugin.get_initial_params(
            service.collector,
            batch.position.contract,
            batch.batch_value,
        )

        batch.order_sequence = params.order_sequence.value
        batch.contract_price = params.contract_price
        batch.spot_price = params.spot_price
        service.set_batch_phase(batch, "FIRST_ORDER_OPEN", ctx["session"], trigger="RULE_INITIALIZE")

        logger.info(
            "Batch %s: params init - order=%s, contract=%s, spot=%s",
            batch.id,
            params.order_sequence.value,
            params.contract_price,
            params.spot_price,
        )

    async def _open_first_order(self, batch):
        ctx = self._get_ctx(batch)
        service = ctx["batch_service"]

        if getattr(batch, "offset", "OPEN") == "CLOSE":
            if batch.order_sequence == "futures_first":
                result = await service.arbitrage_service.close_futures_position(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                )
            else:
                result = await service.arbitrage_service.sell_spot(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                )
        else:
            if batch.order_sequence == "futures_first":
                result = await service.arbitrage_service.open_futures_short(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                    batch.contract_price,
                )
            else:
                result = await service.arbitrage_service.buy_spot(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                    batch.spot_price,
                )

        if not result.success:
            logger.error("Batch %s: First order failed - %s", batch.id, result.message)
            raise Exception(f"Order failed: {result.message}")

        batch.first_side_order_id = str(result.order_id)
        service.set_batch_phase(batch, "FIRST_ORDER_WAIT", ctx["session"], trigger="RULE_OPEN_FIRST_ORDER")
        logger.info("Batch %s: First order placed - %s", batch.id, result.order_id)

    async def _open_second_order(self, batch):
        ctx = self._get_ctx(batch)
        service = ctx["batch_service"]

        if getattr(batch, "offset", "OPEN") == "CLOSE":
            if batch.order_sequence == "futures_first":
                result = await service.arbitrage_service.sell_spot(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                )
            else:
                result = await service.arbitrage_service.close_futures_position(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                )
        else:
            if batch.order_sequence == "futures_first":
                result = await service.arbitrage_service.buy_spot(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                    batch.spot_price,
                )
            else:
                result = await service.arbitrage_service.open_futures_short(
                    service.trader,
                    batch.position.contract,
                    batch.batch_value,
                    batch.contract_price,
                )

        if not result.success:
            logger.error("Batch %s: Second order failed - %s", batch.id, result.message)
            raise Exception(f"Order failed: {result.message}")

        batch.second_side_order_id = str(result.order_id)
        service.set_batch_phase(batch, "SECOND_ORDER_WAIT", ctx["session"], trigger="RULE_OPEN_SECOND_ORDER")
        logger.info("Batch %s: Second order placed - %s", batch.id, result.order_id)

    async def _to_second_order_open(self, batch):
        ctx = self._get_ctx(batch)
        service = ctx["batch_service"]
        service.set_batch_phase(batch, "SECOND_ORDER_OPEN", ctx["session"], trigger="RULE_TO_SECOND_ORDER")

    async def _noop(self, batch):
        _ = batch

    async def _post_action(self, curr_rule: dict[str, Any], model, error: Exception | None):
        _ = curr_rule
        ctx = self._get_ctx(model)
        session = ctx["session"]

        if error is not None:
            await session.rollback()
            return

        await session.commit()

    def _get_ctx(self, batch) -> dict[str, Any]:
        ctx = getattr(batch, "_rule_ctx", None)
        if not isinstance(ctx, dict):
            raise RuntimeError("Missing rule execution context")
        return ctx
