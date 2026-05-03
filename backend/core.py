"""
Core scheduler for position execution using APScheduler.
Handles parameter initialization and order execution.
"""
import asyncio
from datetime import datetime
from typing import Optional
from decimal import Decimal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .modules import create_collector, create_trader, PortfolioManager, LockManager
from .plugins.order_sequence import get_plugin
from .database import get_session, BatchExecute, PositionExecute


# Configuration
SLIPPAGE = Decimal('0.001')  # 0.1% slippage
DEFAULT_ORDER_TIMEOUT = 300  # 5 minutes


class PositionScheduler:
    """Scheduler for position execution using APScheduler."""
    
    def __init__(self, collector_type: str = 'binance', 
                 trader_type: str = 'binance',
                 order_plugin: str = 'futures_first'):
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.portfolio = PortfolioManager()
        self.lock_manager = LockManager()
        self.order_plugin = get_plugin(order_plugin)
        self.scheduler = AsyncIOScheduler()
    
    def start(self):
        """Start scheduler with APScheduler."""
        # Add job to init pending batches every 1 second
        self.scheduler.add_job(
            self._init_pending_batches,
            trigger=IntervalTrigger(seconds=1),
            id='init_pending_batches',
            replace_existing=True
        )
        
        # Add job to execute running batches every 1 second
        self.scheduler.add_job(
            self._execute_running_batches,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        
        self.scheduler.start()
        print("APScheduler started")
    
    def stop(self):
        """Stop scheduler."""
        self.scheduler.shutdown()
        print("APScheduler stopped")
    
    async def _init_pending_batches(self):
        """Initialize pending batches with parameters."""
        session = get_session()
        
        # Get all pending batches
        pending = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'PENDING'
        ).all()
        
        # Track contracts that already have a running batch
        contracts_with_running = set()
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
        for batch in running:
            contracts_with_running.add(batch.position.contract)
        
        for batch in pending:
            try:
                contract = batch.position.contract
                
                # Skip if this contract already has a running batch
                if contract in contracts_with_running:
                    continue
                
                # Get order sequence from plugin
                order_seq = self.order_plugin.get_order_sequence()
                
                # Get price data
                contract_ticker = await self.collector.get_contract_ticker(contract)
                spot_price = await self.collector.get_spot_price(contract)
                
                # Calculate prices with slippage
                if order_seq.value == 'futures_first':
                    contract_price = float(contract_ticker.mark_price * (1 + SLIPPAGE))
                    spot_price_val = float(spot_price.ask_price)
                else:
                    spot_price_val = float(spot_price.ask_price * (1 + SLIPPAGE))
                    contract_price = float(contract_ticker.mark_price)
                
                # Update batch
                batch.order_sequence = order_seq.value
                batch.contract_price = contract_price
                batch.spot_price = spot_price_val
                batch.execute_status = 'RUNNING'
                batch.phase = 'CALCULATED_PRICE'
                batch.updated_at = datetime.utcnow()
                
                session.commit()
                print(f"Batch {batch.id} initialized: contract={contract_price}, spot={spot_price_val}")
                
                # Mark contract as having a running batch
                contracts_with_running.add(contract)
                
            except Exception as e:
                print(f"Error initializing batch {batch.id}: {e}")
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                session.commit()
    
    async def _execute_running_batches(self):
        """Execute running batches."""
        session = get_session()
        
        # Get all running batches
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
        
        # Track contracts that have been processed in this tick
        contracts_processed = set()
        
        for batch in running:
            contract = batch.position.contract
            
            # Skip if we already executed a batch for this contract
            if contract in contracts_processed:
                continue
            
            contracts_processed.add(contract)
            
            try:
                # Check timeout
                elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
                if elapsed > batch.timeout:
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = 'TIMEOUT'
                    session.commit()
                    continue
                
                # Execute based on phase
                await self._execute_phase(batch)
                
            except Exception as e:
                print(f"Error executing batch {batch.id}: {e}")
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                session.commit()
    
    async def _execute_phase(self, batch: BatchExecute):
        """Execute a single phase of batch execution."""
        session = get_session()
        phase = batch.phase
        
        if phase == 'CALCULATED_PRICE':
            if batch.order_sequence == 'futures_first':
                batch.phase = 'FUTURES_ORDER_OPEN'
            else:
                batch.phase = 'SPOT_ORDER_OPEN'
            batch.updated_at = datetime.utcnow()
            session.commit()
        
        elif phase == 'SPOT_ORDER_OPEN':
            amount = batch.batch_value or 1000
            result = await self.trader.buy_spot(
                batch.position.contract,
                amount,
                batch.spot_price
            )
            
            if result.success:
                batch.first_side_order_id = str(result.order_id)
                batch.phase = 'SPOT_WAIT_FILLED'
                batch.updated_at = datetime.utcnow()
                session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                session.commit()
        
        elif phase == 'SPOT_WAIT_FILLED':
            if batch.first_side_order_id:
                order_status = await self.trader.get_order_status(
                    batch.position.contract,
                    int(batch.first_side_order_id)
                )
                
                if order_status.get('status') == 'FILLED':
                    batch.first_side_filled_price = float(order_status.get('avgPrice', batch.spot_price))
                    batch.phase = 'SPOT_TRANSFER'
                    batch.updated_at = datetime.utcnow()
                    session.commit()
        
        elif phase == 'SPOT_TRANSFER':
            result = await self.trader.transfer_to_savings(
                batch.position.contract,
                batch.batch_value or 1000
            )
            
            if result.success:
                batch.phase = 'CONTRACT_ORDER_OPEN'
                batch.updated_at = datetime.utcnow()
                session.commit()
        
        elif phase == 'CONTRACT_ORDER_OPEN':
            amount = batch.batch_value or 1000
            result = await self.trader.open_futures_short(
                batch.position.contract,
                amount,
                batch.contract_price
            )
            
            if result.success:
                batch.second_side_order_id = str(result.order_id)
                batch.phase = 'CONTRACT_WAIT_FILLED'
                batch.updated_at = datetime.utcnow()
                session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                session.commit()
        
        elif phase == 'CONTRACT_WAIT_FILLED':
            if batch.second_side_order_id:
                order_status = await self.trader.get_order_status(
                    batch.position.contract,
                    int(batch.second_side_order_id)
                )
                
                if order_status.get('status') == 'FILLED':
                    batch.second_side_filled_price = float(order_status.get('avgPrice', batch.contract_price))
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = 'SUCCESS'
                    batch.phase = 'COMPLETED'
                    batch.updated_at = datetime.utcnow()
                    session.commit()
                    
                    # Check if all batches completed
                    await self._check_position_complete(batch.position_execute_id)
    
    async def _check_position_complete(self, position_id: int):
        """Check if all batches are completed."""
        session = get_session()
        
        batches = session.query(BatchExecute).filter(
            BatchExecute.position_execute_id == position_id
        ).all()
        
        all_completed = all(b.execute_status == 'COMPLETED' for b in batches)
        
        if all_completed:
            pos = session.query(PositionExecute).filter(
                PositionExecute.id == position_id
            ).first()
            
            if pos:
                # Determine overall reason from batches
                reasons = [b.complete_reason for b in batches]
                if any('TIMEOUT' in r for r in reasons):
                    overall_reason = 'TIMEOUT'
                elif any('ERROR' in r for r in reasons):
                    overall_reason = 'ERROR'
                else:
                    overall_reason = 'SUCCESS'
                
                pos.execute_status = 'COMPLETED'
                pos.complete_reason = overall_reason
                pos.updated_at = datetime.utcnow()
                session.commit()
                
                # Release lock
                await self.lock_manager.release(pos.contract)


class CloseScheduler:
    """Scheduler for position closing using APScheduler."""
    
    def __init__(self, collector_type: str = 'binance',
                 trader_type: str = 'binance',
                 order_plugin: str = 'futures_first'):
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.portfolio = PortfolioManager()
        self.order_plugin = get_plugin(order_plugin)
        self.scheduler = AsyncIOScheduler()
    
    def start(self):
        """Start scheduler with APScheduler."""
        self.scheduler.add_job(
            self._init_close_pending_batches,
            trigger=IntervalTrigger(seconds=1),
            id='init_close_pending_batches',
            replace_existing=True
        )
        
        self.scheduler.add_job(
            self._execute_closing,
            trigger=IntervalTrigger(seconds=1),
            id='execute_closing',
            replace_existing=True
        )
        
        self.scheduler.start()
        print("CloseScheduler started")
    
    def stop(self):
        """Stop scheduler."""
        self.scheduler.shutdown()
        print("CloseScheduler stopped")
    
    async def _init_close_pending_batches(self):
        """Initialize pending close batches with parameters."""
        session = get_session()
        
        pending = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'PENDING',
            BatchExecute.offset == 'CLOSE'
        ).all()
        
        contracts_with_running = set()
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
        for batch in running:
            contracts_with_running.add(batch.position.contract)
        
        for batch in pending:
            try:
                contract = batch.position.contract
                
                if contract in contracts_with_running:
                    continue
                
                order_seq = self.order_plugin.get_order_sequence()
                
                contract_ticker = await self.collector.get_contract_ticker(contract)
                spot_price_data = await self.collector.get_spot_price(contract)
                
                if order_seq.value == 'futures_first':
                    contract_price = float(contract_ticker.mark_price * (1 - SLIPPAGE))
                    spot_price_val = float(spot_price_data.bid_price)
                else:
                    spot_price_val = float(spot_price_data.bid_price * (1 - SLIPPAGE))
                    contract_price = float(contract_ticker.mark_price)
                
                batch.order_sequence = order_seq.value
                batch.contract_price = contract_price
                batch.spot_price = spot_price_val
                batch.execute_status = 'RUNNING'
                batch.phase = 'CALCULATED_PRICE'
                batch.updated_at = datetime.utcnow()
                
                session.commit()
                print(f"Close batch {batch.id} initialized: contract={contract_price}, spot={spot_price_val}")
                
                contracts_with_running.add(contract)
                
            except Exception as e:
                print(f"Error initializing close batch {batch.id}: {e}")
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                session.commit()
    
    async def _execute_closing(self):
        """Execute closing batches."""
        session = get_session()
        
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING',
            BatchExecute.offset == 'CLOSE'
        ).all()
        
        contracts_processed = set()
        
        for batch in running:
            contract = batch.position.contract
            
            if contract in contracts_processed:
                continue
            
            contracts_processed.add(contract)
            
            try:
                elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
                if elapsed > batch.timeout:
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = 'TIMEOUT'
                    session.commit()
                    continue
                
                await self._execute_close_phase(batch)
                
            except Exception as e:
                print(f"Error closing batch {batch.id}: {e}")
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                session.commit()
    
    async def _execute_close_phase(self, batch: BatchExecute):
        """Execute close phase."""
        session = get_session()
        phase = batch.phase
        
        if phase == 'CALCULATED_PRICE':
            if batch.order_sequence == 'futures_first':
                batch.phase = 'CONTRACT_CLOSE'
            else:
                batch.phase = 'SPOT_TRANSFER_FROM_SAVINGS'
            batch.updated_at = datetime.utcnow()
            session.commit()
        
        elif phase == 'SPOT_TRANSFER_FROM_SAVINGS':
            result = await self.trader.transfer_from_savings(
                batch.position.contract,
                batch.batch_value or 1000
            )
            
            if result.success:
                batch.phase = 'SPOT_SELL_ORDER'
                batch.updated_at = datetime.utcnow()
                session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                session.commit()
        
        elif phase == 'SPOT_SELL_ORDER':
            result = await self.trader.sell_spot(
                batch.position.contract,
                batch.batch_value or 1000
            )
            
            if result.success:
                batch.first_side_order_id = str(result.order_id) if result.order_id else ''
                batch.phase = 'SPOT_SELL_WAIT'
                batch.updated_at = datetime.utcnow()
                session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                session.commit()
        
        elif phase == 'SPOT_SELL_WAIT':
            if batch.first_side_order_id:
                order_status = await self.trader.get_order_status(
                    batch.position.contract,
                    int(batch.first_side_order_id)
                )
                
                if order_status.get('status') == 'FILLED':
                    batch.first_side_filled_price = float(order_status.get('avgPrice', batch.spot_price))
                    batch.phase = 'CONTRACT_CLOSE'
                    batch.updated_at = datetime.utcnow()
                    session.commit()
        
        elif phase == 'CONTRACT_CLOSE':
            result = await self.trader.close_futures_position(
                batch.position.contract,
                batch.batch_value or 1000
            )
            
            if result.success:
                batch.second_side_order_id = str(result.order_id) if result.order_id else ''
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = 'SUCCESS'
                batch.phase = 'COMPLETED'
                batch.updated_at = datetime.utcnow()
                session.commit()
                
                await self._check_close_complete(batch.position_execute_id)
    
    async def _check_close_complete(self, position_id: int):
        """Check if all close batches completed."""
        session = get_session()
        
        batches = session.query(BatchExecute).filter(
            BatchExecute.position_execute_id == position_id,
            BatchExecute.offset == 'CLOSE'
        ).all()
        
        all_completed = all(b.execute_status == 'COMPLETED' for b in batches)
        
        if all_completed:
            pos = session.query(PositionExecute).filter(
                PositionExecute.id == position_id
            ).first()
            
            if pos:
                reasons = [b.complete_reason for b in batches]
                if any('TIMEOUT' in r for r in reasons):
                    overall_reason = 'TIMEOUT'
                elif any('ERROR' in r for r in reasons):
                    overall_reason = 'ERROR'
                else:
                    overall_reason = 'SUCCESS'
                
                pos.execute_status = 'COMPLETED'
                pos.complete_reason = overall_reason
                pos.updated_at = datetime.utcnow()
                session.commit()
                
                lock_mgr = LockManager()
                await lock_mgr.release(pos.contract)