"""
Phase state machine using transitions library.
Provides clean state machine logic for batch execution phases.
"""
import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional, Callable

from transitions import Machine
from dataclasses import dataclass, field
from sqlalchemy import select

logger = logging.getLogger(__name__)

from models.database import get_async_session, init_db_async, BatchExecute, PositionExecute
from events.order_watcher import SchedulerOrderWatcher
from services import create_collector, create_trader, PortfolioManager, LockManager
from plugins.order_sequence import get_plugin


# Configuration
SLIPPAGE = Decimal('0.001')  # 0.1% slippage
DEFAULT_ORDER_TIMEOUT = 300  # 5 minutes


# Phase states
class PhaseState:
    """Phase state constants."""
    PENDING = 'PENDING'
    FIRST_ORDER_OPEN = 'FIRST_ORDER_OPEN'
    FIRST_ORDER_WAIT = 'FIRST_ORDER_WAIT'
    FIRST_FILLED = 'FIRST_FILLED'
    SECOND_ORDER_OPEN = 'SECOND_ORDER_OPEN'
    SECOND_ORDER_WAIT = 'SECOND_ORDER_WAIT'
    COMPLETED = 'COMPLETED'


@dataclass
class BatchContext:
    """Batch execution context passed between state machine callbacks."""
    batch_id: int
    position_execute_id: int
    contract: str
    order_sequence: str
    batch_value: float
    timeout: int
    contract_price: float = 0.0
    spot_price: float = 0.0
    first_side_order_id: str = None
    first_side_filled_price: float = 0.0
    second_side_order_id: str = None
    second_side_filled_price: float = 0.0
    complete_reason: str = None


class PhaseStateMachineError(Exception):
    """Phase state machine error."""
    pass


class BatchPhaseMachine:
    """
    Phase state machine for batch execution.
    
    Uses transitions library to manage state flow.
    Each state has enter/exit callbacks for phase logic.
    """
    
    # Define states
    states = [
        PhaseState.PENDING,
        PhaseState.FIRST_ORDER_OPEN,
        PhaseState.FIRST_ORDER_WAIT,
        PhaseState.FIRST_FILLED,
        PhaseState.SECOND_ORDER_OPEN,
        PhaseState.SECOND_ORDER_WAIT,
        PhaseState.COMPLETED
    ]
    
    # Define transitions (from -> to)
    # Note: before callbacks are removed because transitions 0.9.3 doesn't support async callbacks
    # Async operations are handled manually after trigger calls
    transitions_config = [
        # Phase 1: PENDING -> FIRST_ORDER_OPEN
        {
            'trigger': 'initialize_params',
            'source': PhaseState.PENDING,
            'dest': PhaseState.FIRST_ORDER_OPEN,
            'conditions': ['_check_can_initialize']
        },
        # Phase 2: FIRST_ORDER_OPEN -> FIRST_ORDER_WAIT
        {
            'trigger': 'open_first_order',
            'source': PhaseState.FIRST_ORDER_OPEN,
            'dest': PhaseState.FIRST_ORDER_WAIT,
            'conditions': ['_check_can_open_order']
        },
        # Phase 3: FIRST_ORDER_WAIT -> FIRST_FILLED (triggered by OrderWatcher)
        {
            'trigger': 'first_order_filled',
            'source': PhaseState.FIRST_ORDER_WAIT,
            'dest': PhaseState.FIRST_FILLED
        },
        # Phase 3 alt: FIRST_ORDER_WAIT -> FIRST_ORDER_OPEN (retry if cancelled)
        {
            'trigger': 'retry_first_order',
            'source': PhaseState.FIRST_ORDER_WAIT,
            'dest': PhaseState.FIRST_ORDER_OPEN
        },
        # Phase 4: FIRST_FILLED -> SECOND_ORDER_OPEN
        {
            'trigger': 'proceed_to_second',
            'source': PhaseState.FIRST_FILLED,
            'dest': PhaseState.SECOND_ORDER_OPEN
        },
        # Phase 5: SECOND_ORDER_OPEN -> SECOND_ORDER_WAIT
        {
            'trigger': 'open_second_order',
            'source': PhaseState.SECOND_ORDER_OPEN,
            'dest': PhaseState.SECOND_ORDER_WAIT,
            'conditions': ['_check_can_open_order']
        },
        # Phase 6: SECOND_ORDER_WAIT -> COMPLETED (triggered by OrderWatcher)
        {
            'trigger': 'second_order_filled',
            'source': PhaseState.SECOND_ORDER_WAIT,
            'dest': PhaseState.COMPLETED
        },
    ]
    
    def __init__(
        self,
        collector=None,
        trader=None,
        order_plugin=None,
        order_watcher: SchedulerOrderWatcher = None,
        skip_init: bool = False
    ):
        """
        Initialize state machine.
        
        Args:
            collector: Market data collector
            trader: Trading executor
            order_plugin: Order sequence plugin
            order_watcher: Order status watcher
            skip_init: If True, skip auto_initialize (manual mode)
        """
        self.collector = collector or create_collector()
        self.trader = trader or create_trader()
        self.order_plugin = order_plugin or get_plugin()
        self.order_watcher = order_watcher
        
        # Current batch context
        self._context: Optional[BatchContext] = None
        self._batch: Optional[BatchExecute] = None
        
        # Initialize transitions machine
        self.machine = Machine(
            model=self,
            states=BatchPhaseMachine.states,
            transitions=BatchPhaseMachine.transitions_config,
            initial=PhaseState.PENDING,
            auto_triggers=['initialize_params', 'open_first_order', 
                          'first_order_filled', 'retry_first_order',
                          'proceed_to_second', 'open_second_order', 
                          'second_order_filled'],
            send_event=True,
            # If True, trigger won't fire if conditions fail
            # Instead, machine.googles in same state
            # Set to False to have explicit error
            strict=False
        )
        
        # Add callbacks
        self.machine.on_enter_PENDING(self._on_pending)
        self.machine.on_enter_FIRST_ORDER_OPEN(self._on_first_order_open)
        self.machine.on_enter_FIRST_ORDER_WAIT(self._on_first_order_wait)
        self.machine.on_enter_FIRST_FILLED(self._on_first_filled)
        self.machine.on_enter_SECOND_ORDER_OPEN(self._on_second_order_open)
        self.machine.on_enter_SECOND_ORDER_WAIT(self._on_second_order_wait)
        self.machine.on_enter_COMPLETED(self._on_completed)
        
        # Exit callbacks
        self.machine.on_exit_PENDING(self._exit_pending)
        self.machine.on_exit_FIRST_ORDER_OPEN(self._exit_first_order_open)
        self.machine.on_exit_FIRST_ORDER_WAIT(self._exit_first_order_wait)
        self.machine.on_exit_FIRST_FILLED(self._exit_first_filled)
        self.machine.on_exit_SECOND_ORDER_OPEN(self._exit_second_order_open)
        self.machine.on_exit_SECOND_ORDER_WAIT(self._exit_second_order_wait)
    
    # ==================== Loading from DB ====================
    
    @classmethod
    def load_from_batch(cls, batch: BatchExecute, **kwargs) -> 'BatchPhaseMachine':
        """
        Load state machine from existing batch record.
        
        Args:
            batch: BatchExecute record from DB
            **kwargs: Other dependencies (collector, trader, etc.)
        
        Returns:
            Loaded state machine with correct state
        """
        machine = cls(**kwargs)
        
        # Load batch
        machine._batch = batch
        machine._context = BatchContext(
            batch_id=batch.id,
            position_execute_id=batch.position_execute_id,
            contract=batch.position.contract,
            order_sequence=batch.order_sequence or 'futures_first',
            batch_value=batch.batch_value or 1000,
            timeout=batch.timeout or DEFAULT_ORDER_TIMEOUT,
            contract_price=batch.contract_price or 0.0,
            spot_price=batch.spot_price or 0.0,
            first_side_order_id=batch.first_side_order_id,
            first_side_filled_price=batch.first_side_filled_price or 0.0,
            second_side_order_id=batch.second_side_order_id,
            second_side_filled_price=batch.second_side_filled_price or 0.0
        )
        
        # Set state from batch.phase
        if batch.phase:
            state = batch.phase
            if state in cls.states:
                machine.state = state
            else:
                logger.warning(f"Unknown phase: {state}, defaulting to PENDING")
                machine.state = PhaseState.PENDING
        
        return machine
    
    async def save_to_batch(self):
        """Save current state to batch record."""
        if not self._batch:
            return
        
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == self._batch.id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return
            
            # Save state
            batch.phase = self.state
            
            # Save context
            if self._context:
                batch.contract_price = self._context.contract_price
                batch.spot_price = self._context.spot_price
                batch.first_side_order_id = self._context.first_side_order_id
                batch.first_side_filled_price = self._context.first_side_filled_price
                batch.second_side_order_id = self._context.second_side_order_id
                batch.second_side_filled_price = self._context.second_side_filled_price
                batch.order_sequence = self._context.order_sequence
            
            batch.updated_at = datetime.utcnow()
            await session.commit()
    
    # ==================== Conditions ====================
    
    def _check_can_initialize(self, event) -> bool:
        """Check if can initialize parameters."""
        return True
    
    def _check_can_open_order(self, event) -> bool:
        """Check if can open order."""
        return True
    
    # ==================== Before Callbacks ====================
    
    async def _before_init_params(self, event):
        """Initialize parameters before transitioning to FIRST_ORDER_OPEN."""
        await self._initialize_params()
    
    async def _before_open_first_order(self, event):
        """Open first order before transitioning to FIRST_ORDER_WAIT."""
        await self._open_first_order()
    
    async def _before_first_order_filled(self, event):
        """Handle first order filled."""
        # Get filled price from event data
        filled_price = None
        if event and hasattr(event, 'kargs') and event.kargs:
            filled_price = event.kargs.get('filled_price')
        
        await self._handle_first_order_filled(filled_price)
    
    def _before_retry_first_order(self, event):
        """Retry first order."""
        pass  # Simply transition back to OPEN state
    
    def _before_second_order_open(self, event):
        """Prepare second order."""
        pass
    
    async def _before_open_second_order(self, event):
        """Open second order before transitioning to SECOND_ORDER_WAIT."""
        await self._open_second_order()
    
    async def _before_complete(self, event):
        """Complete the batch."""
        filled_price = None
        if event and hasattr(event, 'kargs') and event.kargs:
            filled_price = event.kargs.get('filled_price')
        
        await self._handle_second_order_filled(filled_price)
    
    # ==================== Enter Callbacks (Phase Handlers) ====================
    
    async def _on_pending(self, event):
        """Enter PENDING state."""
        logger.debug(f"Batch {self._context.batch_id}: Entering PENDING")
    
    async def _initialize_params(self):
        """Initialize trading parameters."""
        if not self._context:
            return
        
        # Get order sequence
        order_seq = self.order_plugin.get_order_sequence()
        contract = self._context.contract
        
        # Get prices
        contract_ticker = await self.collector.get_contract_ticker(contract)
        spot_price = await self.collector.get_spot_price(contract)
        
        # Calculate prices with slippage
        if order_seq.value == 'futures_first':
            contract_price = float(contract_ticker.mark_price * (1 + SLIPPAGE))
            spot_price_val = float(spot_price.ask_price)
        else:
            spot_price_val = float(spot_price.ask_price * (1 + SLIPPAGE))
            contract_price = float(contract_ticker.mark_price)
        
        # Update context
        self._context.order_sequence = order_seq.value
        self._context.contract_price = contract_price
        self._context.spot_price = spot_price_val
        
        logger.info(f"Batch {self._context.batch_id}: params init - "
                   f"order={order_seq.value}, contract={contract_price}, spot={spot_price_val}")
        
        # Save to DB
        await self.save_to_batch()
    
    async def _on_first_order_open(self, event):
        """Enter FIRST_ORDER_OPEN state."""
        logger.debug(f"Batch {self._context.batch_id}: Entering FIRST_ORDER_OPEN")
    
    async def _open_first_order(self):
        """Open first order."""
        if not self._context:
            return
        
        amount = self._context.batch_value
        
        if self._context.order_sequence == 'futures_first':
            result = await self.trader.open_futures_short(
                self._context.contract,
                amount,
                self._context.contract_price
            )
        else:
            result = await self.trader.buy_spot(
                self._context.contract,
                amount,
                self._context.spot_price
            )
        
        if result.success:
            self._context.first_side_order_id = str(result.order_id)
            logger.info(f"Batch {self._context.batch_id}: First order placed - {result.order_id}")
        else:
            logger.error(f"Batch {self._context.batch_id}: First order failed - {result.message}")
            raise PhaseStateMachineError(f"Order failed: {result.message}")
        
        await self.save_to_batch()
    
    async def _on_first_order_wait(self, event):
        """Enter FIRST_ORDER_WAIT state - register to OrderWatcher."""
        if not self._context or not self._context.first_side_order_id:
            return
        
        # Register to OrderWatcher
        if self.order_watcher:
            await self.order_watcher.watch_order(
                batch_id=self._context.batch_id,
                order_id=self._context.first_side_order_id,
                symbol=self._context.contract,
                phase='FIRST_ORDER_WAIT',
                timeout=self._context.timeout
            )
        
        logger.info(f"Batch {self._context.batch_id}: Watching first order - {self._context.first_side_order_id}")
    
    async def _handle_first_order_filled(self, filled_price: float = None):
        """Handle first order filled."""
        if filled_price:
            self._context.first_side_filled_price = filled_price
        
        await self.save_to_batch()
        
        logger.info(f"Batch {self._context.batch_id}: First order filled - price={filled_price}")
    
    async def _on_first_filled(self, event):
        """Enter FIRST_FILLED state."""
        logger.debug(f"Batch {self._context.batch_id}: Entering FIRST_FILLED")
        
        # Note: proceed_to_second() is called from batch_service._execute_current_phase
        # This is just an enter callback for logging purposes
        pass
    
    async def _exit_first_filled(self, event):
        """Exit FIRST_FILLED state."""
        pass
    
    async def _on_second_order_open(self, event):
        """Enter SECOND_ORDER_OPEN state."""
        logger.debug(f"Batch {self._context.batch_id}: Entering SECOND_ORDER_OPEN")
    
    async def _open_second_order(self):
        """Open second order."""
        if not self._context:
            return
        
        amount = self._context.batch_value
        
        # Second order is opposite of first
        if self._context.order_sequence == 'futures_first':
            result = await self.trader.buy_spot(
                self._context.contract,
                amount,
                self._context.spot_price
            )
        else:
            result = await self.trader.open_futures_short(
                self._context.contract,
                amount,
                self._context.contract_price
            )
        
        if result.success:
            self._context.second_side_order_id = str(result.order_id)
            logger.info(f"Batch {self._context.batch_id}: Second order placed - {result.order_id}")
        else:
            logger.error(f"Batch {self._context.batch_id}: Second order failed - {result.message}")
            raise PhaseStateMachineError(f"Order failed: {result.message}")
        
        await self.save_to_batch()
    
    async def _on_second_order_wait(self, event):
        """Enter SECOND_ORDER_WAIT state - register to OrderWatcher."""
        if not self._context or not self._context.second_side_order_id:
            return
        
        # Register to OrderWatcher
        if self.order_watcher:
            await self.order_watcher.watch_order(
                batch_id=self._context.batch_id,
                order_id=self._context.second_side_order_id,
                symbol=self._context.contract,
                phase='SECOND_ORDER_WAIT',
                timeout=self._context.timeout
            )
        
        logger.info(f"Batch {self._context.batch_id}: Watching second order - {self._context.second_side_order_id}")
    
    async def _handle_second_order_filled(self, filled_price: float = None):
        """Handle second order filled - transfer to savings."""
        if filled_price:
            self._context.second_side_filled_price = filled_price
        
        # Transfer to savings - calculate actual quantity
        if self._context.order_sequence == 'futures_first':
            # Second side is spot, transfer spot asset
            spot_quantity = self._context.batch_value / self._context.spot_price
        else:
            # Second side is futures, transfer spot asset  
            spot_quantity = self._context.batch_value / self._context.spot_price
        
        asset = self._context.contract.replace('USDT', '')
        transfer_result = await self.trader.transfer_to_savings(
            self._context.contract,
            round(spot_quantity, 6)
        )
        
        self._context.complete_reason = 'SUCCESS'
        
        await self.save_to_batch()
        
        logger.info(f"Batch {self._context.batch_id}: Completed - transferred to savings")
        
        # Check position complete
        await self._check_position_complete()
    
    async def _on_completed(self, event):
        """Enter COMPLETED state."""
        logger.debug(f"Batch {self._context.batch_id}: Entering COMPLETED")
    
    async def _exit_completed(self, event):
        """Exit COMPLETED state."""
        pass
    
    # ==================== Other Callbacks ====================
    
    async def _exit_pending(self, event):
        """Exit PENDING state."""
        pass
    
    async def _exit_first_order_open(self, event):
        """Exit FIRST_ORDER_OPEN state."""
        pass
    
    async def _exit_first_order_wait(self, event):
        """Exit FIRST_ORDER_WAIT state."""
        pass
    
    async def _exit_second_order_open(self, event):
        """Exit SECOND_ORDER_OPEN state."""
        pass
    
    async def _exit_second_order_wait(self, event):
        """Exit SECOND_ORDER_WAIT state."""
        pass
    
    async def _check_position_complete(self):
        """Check if position is complete."""
        if not self._context:
            return
        
        async with get_async_session() as session:
            from sqlalchemy import select
            
            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.position_execute_id == self._context.position_execute_id
                )
            )
            batches = list(result.scalars().all())
            
            if all(b.phase == PhaseState.COMPLETED for b in batches):
                result = await session.execute(
                    select(PositionExecute).where(
                        PositionExecute.id == self._context.position_execute_id
                    )
                )
                pos = result.scalar_one_or_none()
                
                if pos:
                    reasons = [b.complete_reason for b in batches]
                    if 'TIMEOUT' in reasons:
                        overall = 'TIMEOUT'
                    elif any('ERROR' in r for r in reasons if r):
                        overall = 'ERROR'
                    else:
                        overall = 'SUCCESS'
                    
                    pos.execute_status = 'COMPLETED'
                    pos.complete_reason = overall
                    pos.updated_at = datetime.utcnow()
                    await session.commit()
    
    # ==================== Public API ====================
    
    @property
    def state(self) -> str:
        """Get current state."""
        return self.machine.state
    
    @property
    def context(self) -> BatchContext:
        """Get batch context."""
        return self._context
    
    def trigger(self, trigger_name: str, **kwargs):
        """
        Manually trigger a transition.
        
        Args:
            trigger_name: Name of trigger
            **kwargs: Arguments for the transition
        """
        if hasattr(self, trigger_name):
            getattr(self, trigger_name)(**kwargs)
    
    def can_trigger(self, trigger_name: str) -> bool:
        """
        Check if a trigger can be fired.
        
        Args:
            trigger_name: Name of trigger
            
        Returns:
            True if trigger is valid from current state
        """
        return self.machine.get_triggers(self.state) and trigger_name in self.machine.get_triggers(self.state)


# ====================
# OrderWatcher Integration


class SchedulerPhaseMachine:
    """
    Integration: Scheduler -> PhaseMachine.
    Wraps BatchPhaseMachine with scheduler integration.
    """
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
    
    async def start_batch(self, batch: BatchExecute) -> BatchPhaseMachine:
        """
        Start processing a batch.
        
        Args:
            batch: BatchExecute record
            
        Returns:
            Phase machine for this batch
        """
        return BatchPhaseMachine.load_from_batch(
            batch,
            collector=self.scheduler.collector,
            trader=self.scheduler.trader,
            order_plugin=self.scheduler.order_plugin,
            order_watcher=self.scheduler.order_watcher
        )
    
    async def trigger_phase_change(self, batch_id: int, trigger: str, **kwargs):
        """
        Trigger phase change from OrderWatcher callback.
        
        Args:
            batch_id: Batch ID
            trigger: Trigger name
            **kwargs: Trigger arguments
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return
            
            # Load machine
            machine = BatchPhaseMachine.load_from_batch(
                batch,
                collector=self.scheduler.collector,
                trader=self.scheduler.trader,
                order_plugin=self.scheduler.order_plugin,
                order_watcher=self.scheduler.order_watcher
            )
            
            # Check if trigger is valid
            if not machine.can_trigger(trigger):
                logger.warning(f"Cannot trigger {trigger} from state {machine.state}")
                return
            
            # Trigger
            try:
                machine.trigger(trigger, **kwargs)
            except Exception as e:
                logger.error(f"Trigger {trigger} failed: {e}")
                batch.complete_reason = f'ERROR: {str(e)}'
                batch.phase = PhaseState.COMPLETED
                await session.commit()