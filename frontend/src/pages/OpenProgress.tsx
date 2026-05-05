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
  phase_history?: PhaseHistory[];
}

interface PhaseHistory {
  id: number;
  from_phase?: string | null;
  to_phase: string;
  trigger?: string;
  note?: string;
  created_at?: string;
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
  const [manualPositionId, setManualPositionId] = useState<string>(() => {
    const raw = localStorage.getItem('openProgressPositionId');
    return raw || '';
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const queryId = new URLSearchParams(window.location.search).get('id');
  const resolvedPositionId = positionId ?? (queryId ? Number(queryId) : undefined) ?? (manualPositionId ? Number(manualPositionId) : undefined);

  useEffect(() => {
    if (resolvedPositionId && Number.isFinite(resolvedPositionId)) {
      fetchProgress();
      const interval = setInterval(fetchProgress, 5000);
      return () => clearInterval(interval);
    } else {
      setLoading(false);
    }
  }, [resolvedPositionId]);

  const fetchProgress = async () => {
    if (!resolvedPositionId || !Number.isFinite(resolvedPositionId)) return;
    
    try {
      const response = await fetch(`/api/open-progress/${resolvedPositionId}`);
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
      'FIRST_ORDER_OPEN': '第一边挂单中',
      'FIRST_ORDER_WAIT': '等待第一边成交',
      'FIRST_FILLED': '第一边已成交',
      'SECOND_ORDER_OPEN': '第二边挂单中',
      'SECOND_ORDER_WAIT': '等待第二边成交',
      'COMPLETED': '已完成',
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

  if (!resolvedPositionId || !Number.isFinite(resolvedPositionId)) {
    return (
      <div className="open-progress">
        <h1>开仓进度</h1>
        <div className="position-picker">
          <input
            type="number"
            min="1"
            placeholder="输入仓位ID，例如 5"
            value={manualPositionId}
            onChange={(e) => setManualPositionId(e.target.value)}
          />
          <button
            className="refresh-btn"
            type="button"
            onClick={() => {
              if (manualPositionId) {
                localStorage.setItem('openProgressPositionId', manualPositionId);
                fetchProgress();
              }
            }}
          >
            查看进度
          </button>
        </div>
        <p className="no-data">未选择仓位，请输入仓位ID后查看</p>
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
      <div className="page-header">
        <h1>开仓进度</h1>
        <button className="refresh-btn" type="button" onClick={fetchProgress} disabled={loading}>
          刷新进度
        </button>
      </div>
      
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
                    <span className="stage-name">参数初始化</span>
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
                  
                  <div className={`stage ${batch.phase === 'FIRST_FILLED' || !!batch.second_side_order_id || batch.status === 'COMPLETED' ? 'completed' : ''}`}>
                    <span className="stage-name">第一边成交</span>
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
                    <span className="stage-name">完成 ({getPhaseLabel(batch.phase)})</span>
                  </div>
                </div>

                {batch.phase_history && batch.phase_history.length > 0 && (
                  <div className="phase-history">
                    <div className="phase-history-title">阶段变更记录</div>
                    <div className="phase-history-list">
                      {batch.phase_history.map((item) => (
                        <div key={item.id} className="phase-history-item">
                          <span className="phase-transition">
                            {(item.from_phase || 'INIT')} → {item.to_phase}
                          </span>
                          <span className="phase-meta">
                            {item.trigger || 'SYSTEM'}
                            {item.created_at ? ` | ${new Date(item.created_at).toLocaleString()}` : ''}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
