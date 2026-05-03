import { useState } from 'react';
import './OpenPosition.css';

interface OpenPositionProps {
  onSuccess?: (positionId: number) => void;
}

export default function OpenPosition({ onSuccess }: OpenPositionProps) {
  const [contract, setContract] = useState('BTCUSDT');
  const [batchNum, setBatchNum] = useState(1);
  const [batchValue, setBatchValue] = useState(1000);
  const [orderPlugin, setOrderPlugin] = useState('futures_first');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/api/open-position', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contract,
          batch_num: batchNum,
          batch_position_value: batchValue,
          order_plugin: orderPlugin
        })
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Failed to open position');
      }

      const data = await response.json();
      if (onSuccess) {
        onSuccess(data.position_id);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="open-position">
      <h1>开仓</h1>
      <p className="subtitle">做空合约 + 买入现货 -> Delta中性套利</p>

      <form onSubmit={handleSubmit}>
        {error && <div className="error">{error}</div>}

        <div className="form-group">
          <label>合约</label>
          <select
            value={contract}
            onChange={(e) => setContract(e.target.value)}
            required
          >
            <option value="BTCUSDT">BTCUSDT</option>
            <option value="ETHUSDT">ETHUSDT</option>
            <option value="BNBUSDT">BNBUSDT</option>
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

        <div className="form-group">
          <label>执行顺序</label>
          <select
            value={orderPlugin}
            onChange={(e) => setOrderPlugin(e.target.value)}
          >
            <option value="futures_first">先合约后现货</option>
            <option value="spot_first">先现货后合约</option>
          </select>
        </div>

        <button type="submit" disabled={loading}>
          {loading ? '提交中...' : '确认开仓'}
        </button>
      </form>

      <div className="info">
        <h3>说明</h3>
        <ul>
          <li>做空合约收取资金费率</li>
          <li>买入现货并存入活期理财</li>
          <li>实现Delta中性对冲</li>
        </ul>
      </div>
    </div>
  );
}