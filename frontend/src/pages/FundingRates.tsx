import { useState, useEffect } from 'react';
import './FundingRates.css';

interface FundingRate {
  symbol: string;
  rate: number;
  next_funding_time: number;
}

interface FundingSummaryItem {
  symbol: string;
  records: number;
  total_rate: number;
  avg_rate: number;
  last_recorded_at: string | null;
}

interface FundingSummary {
  days: number;
  start_time: string;
  symbols: FundingSummaryItem[];
}

export default function FundingRates() {
  const [rates, setRates] = useState<FundingRate[]>([]);
  const [summary, setSummary] = useState<FundingSummary | null>(null);
  const [minTotalRatePct, setMinTotalRatePct] = useState<number | ''>('');
  const [maxTotalRatePct, setMaxTotalRatePct] = useState<number | ''>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchFundingRates();
    const interval = setInterval(fetchFundingRates, 60000);
    return () => clearInterval(interval);
  }, []);

  const fetchFundingRates = async () => {
    try {
      const [ratesRes, summaryRes] = await Promise.all([
        fetch('/api/funding-rates'),
        fetch('/api/funding-rates/summary?days=10'),
      ]);

      if (!ratesRes.ok) throw new Error('Failed to fetch rates');
      if (!summaryRes.ok) throw new Error('Failed to fetch summary');

      const ratesData = await ratesRes.json();
      const summaryData = await summaryRes.json();
      setRates(ratesData);
      setSummary(summaryData);
      setError(null);
    } catch (err) {
      setError('Failed to load funding rates');
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = async () => {
    setLoading(true);
    await fetchFundingRates();
  };

  const filteredSummary = (summary?.symbols || [])
    .filter((item) => {
      const pct = item.total_rate * 100;
      if (minTotalRatePct !== '' && pct < minTotalRatePct) return false;
      if (maxTotalRatePct !== '' && pct > maxTotalRatePct) return false;
      return true;
    })
    .sort((a, b) => b.total_rate - a.total_rate);

  // Sort by rate descending
  const sortedRates = [...rates].sort((a, b) => b.rate - a.rate);

  // Filter positive rates (we want to short)
  const positiveRates = sortedRates.filter(r => r.rate > 0);

  const formatRate = (rate: number) => {
    return (rate * 100).toFixed(4) + '%';
  };

  const formatTime = (timestamp: number) => {
    const date = new Date(timestamp * 1000);
    return date.toLocaleString();
  };

  if (loading) {
    return <div className="loading">Loading...</div>;
  }

  return (
    <div className="funding-rates">
      <h1>资金费率列表</h1>
      <p className="subtitle">正资金费率 = 做空收取费用 = 我们的收益</p>

      <div className="summary-controls">
        <label htmlFor="min-total-rate">总费率下限(%)</label>
        <input
          id="min-total-rate"
          type="number"
          step="0.0001"
          value={minTotalRatePct}
          onChange={(e) => setMinTotalRatePct(e.target.value === '' ? '' : Number(e.target.value))}
        />
        <label htmlFor="max-total-rate">总费率上限(%)</label>
        <input
          id="max-total-rate"
          type="number"
          step="0.0001"
          value={maxTotalRatePct}
          onChange={(e) => setMaxTotalRatePct(e.target.value === '' ? '' : Number(e.target.value))}
        />
        <button type="button" onClick={handleRefresh}>刷新数据</button>
      </div>
      
      {error && <div className="error">{error}</div>}

      <div className="rates-table">
        <h2>近10天合约资金费率汇总（从高到低）</h2>
        {filteredSummary.length ? (
          <table>
            <thead>
              <tr>
                <th>合约</th>
                <th>记录数</th>
                <th>总费率</th>
                <th>平均费率</th>
                <th>最近记录时间</th>
              </tr>
            </thead>
            <tbody>
              {filteredSummary.map((item) => (
                <tr key={item.symbol}>
                  <td className="symbol">{item.symbol}</td>
                  <td>{item.records}</td>
                  <td className={item.total_rate >= 0 ? 'positive' : 'negative'}>
                    {(item.total_rate * 100).toFixed(4)}%
                  </td>
                  <td className={item.avg_rate >= 0 ? 'positive' : 'negative'}>
                    {(item.avg_rate * 100).toFixed(6)}%
                  </td>
                  <td className="time">{item.last_recorded_at ? new Date(item.last_recorded_at).toLocaleString() : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="no-data">暂无符合筛选条件的汇总记录</div>
        )}
      </div>
      
      <div className="rates-table">
        <table>
          <thead>
            <tr>
              <th>合约</th>
              <th>当前资金费率</th>
              <th>下次结算时间</th>
            </tr>
          </thead>
          <tbody>
            {positiveRates.map(rate => (
              <tr key={rate.symbol} className={rate.rate > 0.001 ? 'high-rate' : ''}>
                <td className="symbol">{rate.symbol}</td>
                <td className={`rate ${rate.rate > 0 ? 'positive' : 'negative'}`}>
                  {formatRate(rate.rate)}
                </td>
                <td className="time">{formatTime(rate.next_funding_time)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        
        {positiveRates.length === 0 && (
          <div className="no-data">暂无正资金费率合约</div>
        )}
      </div>
    </div>
  );
}
