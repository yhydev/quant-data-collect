import { useEffect, useMemo, useState } from 'react';
import './OpenPosition.css';

interface OpenPositionProps {
  onSuccess?: (positionId: number) => void;
}

export default function OpenPosition({ onSuccess }: OpenPositionProps) {
  const [contract, setContract] = useState('BTCUSDT');
  const [contracts, setContracts] = useState<string[]>([]);
  const [contractsLoading, setContractsLoading] = useState(true);
  const [batchNum, setBatchNum] = useState(1);
  const [batchValue, setBatchValue] = useState(6);
  const [orderPlugin, setOrderPlugin] = useState('futures_first');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchContracts = async () => {
    try {
      setContractsLoading(true);
      const response = await fetch('/api/contracts');
      if (!response.ok) throw new Error('Failed to load contracts');
      const data = await response.json();
      const list = Array.isArray(data) ? data : [];
      setContracts(list);
      if (list.length > 0 && !list.includes(contract)) {
        setContract(list[0]);
      }
    } catch {
      setContracts([]);
    } finally {
      setContractsLoading(false);
    }
  };

  useEffect(() => {
    fetchContracts();
  }, []);

  const contractOptions = useMemo(() => {
    const set = new Set(contracts);
    if (contract) set.add(contract.toUpperCase());
    return Array.from(set);
  }, [contracts, contract]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/api/open-position', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contract: contract.trim().toUpperCase(),
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
      <div className="page-header">
        <h1>开仓</h1>
        <button className="refresh-btn" type="button" onClick={fetchContracts} disabled={contractsLoading || loading}>
          {contractsLoading ? '刷新中...' : '刷新合约'}
        </button>
      </div>
      <p className="subtitle">做空合约 + 买入现货 {'->'} Delta中性套利</p>

      <form onSubmit={handleSubmit}>
        {error && <div className="error">{error}</div>}

        <div className="form-group">
          <label>合约</label>
          <input
            list="contract-options"
            value={contract}
            onChange={(e) => setContract(e.target.value.toUpperCase())}
            placeholder={contractsLoading ? '加载合约中...' : '输入或选择合约，如 BTCUSDT'}
            required
          />
          <datalist id="contract-options">
            {contractOptions.map((item) => (
              <option key={item} value={item} />
            ))}
          </datalist>
          {!contractsLoading && contracts.length > 0 && (
            <div className="field-hint">可选合约：{contracts.length} 个，支持直接输入</div>
          )}
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
            min="6"
            step="1"
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
