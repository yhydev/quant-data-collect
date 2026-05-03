"""
Core scheduler for position execution using APScheduler.
Each phase is extracted into independent methods for testability.
"""
from datetime import datetime
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
        # Job 1: 唤醒pending批次
        self.scheduler.add_job(
            self._wake_pending_batches,
            trigger=IntervalTrigger(seconds=1),
            id='wake_pending_batches',
            replace_existing=True
        )
        
        # Job 2: 执行批次
        self.scheduler.add_job(
            self._execute_running_batches,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        
        self.scheduler.start()
        print("APScheduler started with 2 jobs")
    
    def stop(self):
        """Stop scheduler."""
        self.scheduler.shutdown()
        print("APScheduler stopped")
    
    # ===== Job 1: 唤醒pending批次 =====
    async def _wake_pending_batches(self):
        """
        定时任务1: 唤醒pending批次
        - 将没有RUNNING的合约的PENDING批次转为RUNNING
        - 同一合约只唤醒ID最小的批次
        """
        session = get_session()
        
        # 获取已有RUNNING的合约
        contracts_running = set()
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
        for batch in running:
            contracts_running.add(batch.position.contract)
        
        # 按ID排序，唤醒最小的
        pending = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'PENDING'
        ).order_by(BatchExecute.id).all()
        
        woken = 0
        for batch in pending:
            contract = batch.position.contract
            
            if contract in contracts_running:
                continue
            
            # 唤醒
            batch.execute_status = 'RUNNING'
            batch.phase = 'PENDING'
            batch.updated_at = datetime.utcnow()
            session.commit()
            
            contracts_running.add(contract)
            woken += 1
            print(f"Batch {batch.id} woken: contract={contract}")
        
        if woken > 0:
            print(f"Woke {woken} pending batches")
    
    # ===== Job 2: 执行批次 =====
    async def _execute_running_batches(self):
        """
        定时任务2: 执行批次
        - 按ID升序处理每个RUNNING批次
        - 同一合约只处理ID最小的
        - 检查超时后调用 _execute_phase()
        """
        session = get_session()
        
        # 按ID排序获取RUNNING批次
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).order_by(BatchExecute.id).all()
        
        contracts_processed = set()
        
        for batch in running:
            contract = batch.position.contract
            
            if contract in contracts_processed:
                continue
            contracts_processed.add(contract)
            
            try:
                # 检查超时
                elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
                if elapsed > batch.timeout:
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = 'TIMEOUT'
                    session.commit()
                    continue
                
                # 执行批次
                await self._execute_phase(batch)
                
            except Exception as e:
                print(f"Error executing batch {batch.id}: {e}")
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                session.commit()
    
    # ===== Phase执行：按当前phase调用对应方法 =====
    async def _execute_phase(self, batch: BatchExecute):
        """根据phase执行对应阶段"""
        phase = batch.phase
        
        if phase == 'PENDING':
            await self._phase_init_params(batch)
        elif phase == 'FIRST_ORDER_OPEN':
            await self._phase_first_order_open(batch)
        elif phase == 'FIRST_ORDER_WAIT':
            await self._phase_first_order_wait(batch)
        elif phase == 'FIRST_FILLED':
            await self._phase_first_filled(batch)
        elif phase == 'SPOT_TRANSFER':
            await self._phase_spot_transfer(batch)
        elif phase == 'SECOND_ORDER_OPEN':
            await self._phase_second_order_open(batch)
        elif phase == 'SECOND_ORDER_WAIT':
            await self._phase_second_order_wait(batch)
    
    # ===== 阶段1: 初始化参数 =====
    async def _phase_init_params(self, batch: BatchExecute):
        """
        阶段1: 初始化参数
        - 获取订单顺序（futures_first / spot_first）
        - 获取合约价格（Mark Price）和现货价格（Ask Price）
        - 计算挂单价（含0.1%滑点）
        - 保存到批次记录
        """
        session = get_session()
        
        # 获取订单顺序
        order_seq = self.order_plugin.get_order_sequence()
        contract = batch.position.contract
        
        # 获取价格
        contract_ticker = await self.collector.get_contract_ticker(contract)
        spot_price = await self.collector.get_spot_price(contract)
        
        # 计算挂单价（含滑点）
        if order_seq.value == 'futures_first':
            contract_price = float(contract_ticker.mark_price * (1 + SLIPPAGE))
            spot_price_val = float(spot_price.ask_price)
        else:
            spot_price_val = float(spot_price.ask_price * (1 + SLIPPAGE))
            contract_price = float(contract_ticker.mark_price)
        
        # 保存参数
        batch.order_sequence = order_seq.value
        batch.contract_price = contract_price
        batch.spot_price = spot_price_val
        batch.phase = 'FIRST_ORDER_OPEN'
        batch.updated_at = datetime.utcnow()
        session.commit()
        
        print(f"Batch {batch.id} params: order={order_seq.value}, contract={contract_price}, spot={spot_price_val}")
    
    # ===== 阶段2: 第一边挂单 =====
    async def _phase_first_order_open(self, batch: BatchExecute):
        """
        阶段2: 第一边挂单
        - futures_first: 做空合约（open_futures_short）
        - spot_first: 买入现货（buy_spot）
        - 保存订单ID，进入等待阶段
        """
        session = get_session()
        amount = batch.batch_value or 1000
        
        if batch.order_sequence == 'futures_first':
            result = await self.trader.open_futures_short(
                batch.position.contract,
                amount,
                batch.contract_price
            )
        else:
            result = await self.trader.buy_spot(
                batch.position.contract,
                amount,
                batch.spot_price
            )
        
        if result.success:
            batch.first_side_order_id = str(result.order_id)
            batch.phase = 'FIRST_ORDER_WAIT'
            batch.updated_at = datetime.utcnow()
            session.commit()
        else:
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = f'ERROR: {result.message}'
            session.commit()
    
    # ===== 阶段3: 第一边等待成交 =====
    async def _phase_first_order_wait(self, batch: BatchExecute):
        """
        阶段3: 第一边等待成交
        - 轮询订单状态
        - 成交后记录成交价，进入下一阶段
        """
        session = get_session()
        
        if not batch.first_side_order_id:
            return
        
        order_status = await self.trader.get_order_status(
            batch.position.contract,
            int(batch.first_side_order_id)
        )
        
        if order_status.get('status') == 'FILLED':
            batch.first_side_filled_price = float(
                order_status.get('avgPrice', 
                batch.contract_price if batch.order_sequence == 'futures_first' else batch.spot_price)
            )
            batch.phase = 'FIRST_FILLED'
            batch.updated_at = datetime.utcnow()
            session.commit()
    
    # ===== 阶段4: 第一边已成交 =====
    async def _phase_first_filled(self, batch: BatchExecute):
        """
        阶段4: 第一边已成交
        - futures_first: 转入理财，然后做现货
        - spot_first: 直接做空合约
        """
        session = get_session()
        
        if batch.order_sequence == 'futures_first':
            batch.phase = 'SPOT_TRANSFER'
        else:
            batch.phase = 'SECOND_ORDER_OPEN'
        
        batch.updated_at = datetime.utcnow()
        session.commit()
    
    # ===== 阶段5: 现货转入理财 =====
    async def _phase_spot_transfer(self, batch: BatchExecute):
        """
        阶段5: 现货转入理财
        - 仅 futures_first 路径需要
        - 转入活期理财，然后做第二边
        """
        session = get_session()
        
        result = await self.trader.transfer_to_savings(
            batch.position.contract,
            batch.batch_value or 1000
        )
        
        if result.success:
            batch.phase = 'SECOND_ORDER_OPEN'
            batch.updated_at = datetime.utcnow()
            session.commit()
    
    # ===== 阶段6: 第二边挂单 =====
    async def _phase_second_order_open(self, batch: BatchExecute):
        """
        阶段6: 第二边挂单
        - futures_first: 买入现货（补现货）
        - spot_first: 做空合约（补合约）
        """
        session = get_session()
        amount = batch.batch_value or 1000
        
        if batch.order_sequence == 'futures_first':
            result = await self.trader.buy_spot(
                batch.position.contract,
                amount,
                batch.spot_price
            )
        else:
            result = await self.trader.open_futures_short(
                batch.position.contract,
                amount,
                batch.contract_price
            )
        
        if result.success:
            batch.second_side_order_id = str(result.order_id)
            batch.phase = 'SECOND_ORDER_WAIT'
            batch.updated_at = datetime.utcnow()
            session.commit()
        else:
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = f'ERROR: {result.message}'
            session.commit()
    
    # ===== 阶段7: 第二边等待成交 =====
    async def _phase_second_order_wait(self, batch: BatchExecute):
        """
        阶段7: 第二边等待成交
        - 轮询订单状态
        - 成交后记录成交价，标记完成
        - 检查主记录是否全部完成
        """
        session = get_session()
        
        if not batch.second_side_order_id:
            return
        
        order_status = await self.trader.get_order_status(
            batch.position.contract,
            int(batch.second_side_order_id)
        )
        
        if order_status.get('status') == 'FILLED':
            batch.second_side_filled_price = float(
                order_status.get('avgPrice',
                batch.spot_price if batch.order_sequence == 'futures_first' else batch.contract_price)
            )
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'SUCCESS'
            batch.phase = 'COMPLETED'
            batch.updated_at = datetime.utcnow()
            session.commit()
            
            # 检查主记录完成状态
            await self._check_position_complete(batch.position_execute_id)
    
    # ===== 检查主记录完成状态 =====
    async def _check_position_complete(self, position_id: int):
        """检查主记录的所有批次是否都完成"""
        session = get_session()
        
        batches = session.query(BatchExecute).filter(
            BatchExecute.position_execute_id == position_id
        ).all()
        
        if all(b.execute_status == 'COMPLETED' for b in batches):
            pos = session.query(PositionExecute).filter(
                PositionExecute.id == position_id
            ).first()
            
            if pos:
                reasons = [b.complete_reason for b in batches]
                if 'TIMEOUT' in reasons:
                    overall = 'TIMEOUT'
                elif any('ERROR' in r for r in reasons):
                    overall = 'ERROR'
                else:
                    overall = 'SUCCESS'
                
                pos.execute_status = 'COMPLETED'
                pos.complete_reason = overall
                pos.updated_at = datetime.utcnow()
                session.commit()
                
                await self.lock_manager.release(pos.contract)


# ===== 平仓调度器 (预留) =====
class CloseScheduler:
    """平仓调度器（预留）"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
    
    def start(self):
        self.scheduler.add_job(self._wake_pending_closes, IntervalTrigger(seconds=1), id='wake_pending_closes')
        self.scheduler.add_job(self._execute_closes, IntervalTrigger(seconds=1), id='execute_closes')
        self.scheduler.start()
    
    def stop(self):
        self.scheduler.shutdown()