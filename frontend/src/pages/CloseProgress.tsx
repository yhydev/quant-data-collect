import { useState, useEffect } from 'react';
import './CloseProgress.css';

interface Batch {
  id: number;
  status: string;
  phase: string;
  complete_reason?: string;
}

interface PositionProgress {
  id: number;
  contract: string;
  execute_status: string;
  complete_reason?: string;
  batches: Batch[];
}

interface CloseProgressProps {
  positionId?: number;
}

export default function CloseProgress({ positionId }: CloseProgressProps) {
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

  if (!positionId) {
    return (
      <div className="close-progress">
        <h1>平仓进度</h1>
        <p className="no-data">请先创建平仓任务</p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="close-progress">
        <h1>平仓进度</h1>
        <div className="loading">加载中...</div>
      </div>
    );
  }

  return (
    <div className="close-progress">
      <h1>平仓进度</h1>
      
      {error && <div className="error">{error}</div>}

      {data && (
        <div className="info">
          <p>合约: {data.contract}</p>
          <p>状态: {data.execute_status}</p>
          {data.complete_reason && <p>完成原因: {data.complete_reason}</p>}
          <p>批次进度: {data.batches.filter((b) => b.status === 'COMPLETED').length} / {data.batches.length}</p>
        </div>
      )}
    </div>
  );
}
