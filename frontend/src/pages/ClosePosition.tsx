import { useState, useEffect } from 'react';
import './ClosePosition.css';

interface Position {
  id: number;
  contract: string;
  batch_num: number;
  execute_status: string;
  batch_position_value: number;
}

interface ClosePositionProps {
  onSuccess?: (positionId: number) => void;
}

export default function ClosePosition({ onSuccess }: ClosePositionProps) {
  const [positions, setPositions] = useState<Position[]>([]);
  const [selectedPosition, setSelectedPosition] = useState<number | null>(null);
  const [batchNum, setBatchNum] = useState(1);
  const [batchValue, setBatchValue] = useState(1000);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchPositions();
  }, []);

  const fetchPositions = async () => {
    try {
      const response = await fetch('/api/positions');
      if (!response.ok) throw new Error('Failed to fetch');
      const data = await response.json();
      setPositions(data);
    } catch (err) {
      console.error(err);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedPosition) return;
    
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/api/close-position', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          position_id: selectedPosition,
          batch_num: batchNum,
          batch_position_value: batchValue
        })
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Failed to close position');
      }

      const data = await response.json();
      if (onSuccess) {
        onSuccess(selectedPosition);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="close-position">
      <h1>平仓</h1>
      <p className="subtitle">卖出持仓 + 平合约 -> 完成套利</p>

      <form onSubmit={handleSubmit}>
        {error && <div className="error">{error}</div>}

        <div className="form-group">
          <label>选择持仓</label>
          <select
            value={selectedPosition || ''}
            onChange={(e) => setSelectedPosition(Number(e.target.value))}
            required
          >
            <option value="">选择持仓</option>
            {positions.map(pos => (
              <option key={pos.id} value={pos.id}>
                {pos.contract} - {pos.batch_num}批 - {pos.batch_position_value}USDT
              </option>
            ))}
          </select>
        </div>

        <div className="form-group">
          <label>批次数</label>
          <input
            type="number"
            min="1"
            max="10"
            value={batchNum}
            onChange={(e) => setBatchNum(Number(e.target.value))}
            required
          />
        </div>

        <div className="form-group">
          <label>每批金额 (USDT)</label>
          <input
            type="number"
            min="100"
            step="100"
            value={batchValue}
            onChange={(e) => setBatchValue(Number(e.target.value))}
            required
          />
        </div>

        <button type="submit" disabled={loading || !selectedPosition}>
          {loading ? '提交中...' : '确认平仓'}
        </button>
      </form>
    </div>
  );
}