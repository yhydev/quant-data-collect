import { useState, useEffect } from 'react';
import './FundingRates.css';

interface FundingRate {
  symbol: string;
  rate: number;
  next_funding_time: number;
}

export default function FundingRates() {
  const [rates, setRates] = useState<FundingRate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchFundingRates();
    const interval = setInterval(fetchFundingRates, 60000);
    return () => clearInterval(interval);
  }, []);

  const fetchFundingRates = async () => {
    try {
      const response = await fetch('/api/funding-rates');
      if (!response.ok) throw new Error('Failed to fetch');
      const data = await response.json();
      setRates(data);
      setError(null);
    } catch (err) {
      setError('Failed to load funding rates');
    } finally {
      setLoading(false);
    }
  };

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
      
      {error && <div className="error">{error}</div>}
      
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