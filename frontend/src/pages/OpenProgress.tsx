import { useState, useEffect } from 'react';
import './OpenProgress.css';

interface Batch {
  id: number;
  status: string;
  phase: string;
  contract_price?: number;
  spot_price?: number;
  first_side_order_id?: string;
  first_side_filled_price?: number;
  second_side_order_id?: string;
  second_side_filled_price?: number;
  complete_reason?: string;
}

interface PositionProgress {
  id: number;
  contract: string;
  execute_status: string;
  complete_reason?: string;
  batches: Batch[];
}

interface OpenProgressProps {
  positionId?: number;
}

export default function OpenProgress({ positionId }: OpenProgressProps) {
  const [data, setData] = useState<PositionProgress | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (positionId) {
      fetchProgress();
      const interval = setInterval(fetchProgress, 5000);
      return () => clearInterval(interval);
    }
  }, [positionId]);

  const fetchProgress = async () => {
    if (!positionId) return;
    
    try {
      const response = await fetch(`/api/open-progress/${positionId}`);
      if (!response.ok) throw new Error('Failed to fetch');
      const result = await response.json();
      setData(result);
      setError(null);
    } catch (err) {
      setError('Failed to load progress');
    } finally {
      setLoading(false);
    }
  };

  const getPhaseLabel = (phase: string) => {
    const labels: Record<string, string> = {
      'PENDING': '待执行',
      'CALCULATED_PRICE': '已计算价格',
      'SPOT_ORDER_OPEN': '现货挂单中',
      'SPOT_WAIT_FILLED': '等待现货成交',
      'SPOT_TRANSFER': '转入理财',
      'CONTRACT_ORDER_OPEN': '合约挂单中',
      'CONTRACT_WAIT_FILLED': '等待合约成交',
      'COMPLETED': '已完成',
      'CLOSED': '已平仓'
    };
    return labels[phase] || phase;
  };

  const getStatusClass = (status: string) => {
    const classes: Record<string, string> = {
      'PENDING': 'status-pending',
      'RUNNING': 'status-running',
      'COMPLETED': 'status-completed',
      'FAILED': 'status-failed'
    };
    return classes[status] || '';
  };

  if (!positionId) {
    return (
      <div className="open-progress">
        <h1>开仓进度</h1>
        <p className="no-data">请先创建开仓任务</p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="open-progress">
        <h1>开仓进度</h1>
        <div className="loading">加载中...</div>
      </div>
    );
  }

  return (
    <div className="open-progress">
      <h1>开仓进度</h1>
      
      {error && <div className="error">{error}</div>}
      
      {data && (
        <>
          <div className="position-info">
            <div className="info-item">
              <span className="label">合约:</span>
              <span className="value">{data.contract}</span>
            </div>
            <div className="info-item">
              <span className="label">状态:</span>
              <span className={`value status ${getStatusClass(data.execute_status)}`}>
                {data.execute_status}
              </span>
            </div>
            {data.complete_reason && (
              <div className="info-item">
                <span className="label">完成原因:</span>
                <span className="value">{data.complete_reason}</span>
              </div>
            )}
          </div>

          <div className="batches">
            <h2>批次详情</h2>
            {data.batches.map((batch, index) => (
              <div key={batch.id} className={`batch ${getStatusClass(batch.status)}`}>
                <div className="batch-header">
                  <span className="batch-number">批次 {index + 1}</span>
                  <span className={`status ${getStatusClass(batch.status)}`}>
                    {batch.status}
                  </span>
                </div>
                
                <div className="batch-stages">
                  <div className={`stage ${batch.phase !== 'PENDING' ? 'completed' : ''}`}>
                    <span className="stage-name">计算价格</span>
                    {batch.contract_price && (
                      <span className="stage-price">合约: {batch.contract_price}</span>
                    )}
                    {batch.spot_price && (
                      <span className="stage-price">现货: {batch.spot_price}</span>
                    )}
                  </div>
                  
                  <div className={`stage ${batch.first_side_order_id ? 'completed' : ''}`}>
                    <span className="stage-name">第一边挂单</span>
                    {batch.first_side_order_id && (
                      <span className="stage-order">订单: {batch.first_side_order_id}</span>
                    )}
                    {batch.first_side_filled_price && (
                      <span className="stage-price">成交价: {batch.first_side_filled_price}</span>
                    )}
                  </div>
                  
                  <div className={`stage ${batch.phase === 'SPOT_TRANSFER' || batch.phase === 'CONTRACT_ORDER_OPEN' ? 'completed' : ''}`}>
                    <span className="stage-name">转入理财</span>
                  </div>
                  
                  <div className={`stage ${batch.second_side_order_id ? 'completed' : ''}`}>
                    <span className="stage-name">第二边挂单</span>
                    {batch.second_side_order_id && (
                      <span className="stage-order">订单: {batch.second_side_order_id}</span>
                    )}
                    {batch.second_side_filled_price && (
                      <span className="stage-price">成交价: {batch.second_side_filled_price}</span>
                    )}
                  </div>
                  
                  <div className={`stage ${batch.status === 'COMPLETED' ? 'completed' : ''}`}>
                    <span className="stage-name">完成</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}