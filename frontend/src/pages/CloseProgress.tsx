import { useState, useEffect } from 'react';
import './CloseProgress.css';

interface CloseProgressProps {
  positionId?: number;
}

export default function CloseProgress({ positionId }: CloseProgressProps) {
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
      
      <div className="info">
        <p>平仓流程：</p>
        <ol>
          <li>卖出现货持仓</li>
          <li>买入平合约空头</li>
          <li>完成套利</li>
        </ol>
      </div>
    </div>
  );
}