import { useState, useEffect } from 'react';
import './Dashboard.css';

interface Position {
  id: number;
  contract: string;
  batch_num: number;
  execute_status: string;
  batch_position_value: number;
  created_at: string;
}

interface Earning {
  id: number;
  contract: string;
  amount: number;
  funding_earn: number;
  interest_earn: number;
  pnl: number;
  total_earn: number;
  status: string;
  created_at: string;
}

export default function Dashboard() {
  const [positions, setPositions] = useState<Position[]>([]);
  const [earnings, setEarnings] = useState<Earning[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [positionsRes, earningsRes] = await Promise.all([
        fetch('/api/positions'),
        fetch('/api/earnings')
      ]);

      if (positionsRes.ok) {
        const positionsData = await positionsRes.json();
        setPositions(positionsData);
      }

      if (earningsRes.ok) {
        const earningsData = await earningsRes.json();
        setEarnings(earningsData);
      }
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const totalValue = positions.reduce((sum, p) => sum + p.batch_num * p.batch_position_value, 0);
  const totalEarnings = earnings.reduce((sum, e) => sum + e.total_earn, 0);
  const fundingEarnings = earnings.reduce((sum, e) => sum + e.funding_earn, 0);
  const interestEarnings = earnings.reduce((sum, e) => sum + e.interest_earn, 0);

  if (loading) {
    return (
      <div className="dashboard">
        <div className="loading">加载中...</div>
      </div>
    );
  }

  return (
    <div className="dashboard">
      <h1>监控面板</h1>

      <div className="stats">
        <div className="stat-card">
          <div className="stat-label">当前持仓</div>
          <div className="stat-value">{positions.length}</div>
          <div className="stat-unit">个</div>
        </div>
        
        <div className="stat-card">
          <div className="stat-label">持仓总额</div>
          <div className="stat-value">{totalValue.toLocaleString()}</div>
          <div className="stat-unit">USDT</div>
        </div>
        
        <div className="stat-card">
          <div className="stat-label">资金费率收益</div>
          <div className="stat-value">{fundingEarnings.toFixed(2)}</div>
          <div className="stat-unit">USDT</div>
        </div>
        
        <div className="stat-card">
          <div className="stat-label">理财收益</div>
          <div className="stat-value">{interestEarnings.toFixed(2)}</div>
          <div className="stat-unit">USDT</div>
        </div>
        
        <div className="stat-card highlight">
          <div className="stat-label">总收益</div>
          <div className="stat-value">{totalEarnings.toFixed(2)}</div>
          <div className="stat-unit">USDT</div>
        </div>
      </div>

      <div className="section">
        <h2>当前持仓</h2>
        {positions.length === 0 ? (
          <div className="no-data">暂无持仓</div>
        ) : (
          <div className="position-list">
            {positions.map(pos => (
              <div key={pos.id} className="position-item">
                <div className="position-contract">{pos.contract}</div>
                <div className="position-info">
                  {pos.batch_num}批 × {pos.batch_position_value}USDT
                </div>
                <div className={`position-status status-${pos.execute_status.toLowerCase()}`}>
                  {pos.execute_status}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="section">
        <h2>收益历史</h2>
        {earnings.length === 0 ? (
          <div className="no-data">暂无收益记录</div>
        ) : (
          <table className="earnings-table">
            <thead>
              <tr>
                <th>合约</th>
                <th>金额</th>
                <th>资金费率</th>
                <th>理财</th>
                <th>总价差</th>
                <th>总收益</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {earnings.slice(-5).reverse().map(earn => (
                <tr key={earn.id}>
                  <td>{earn.contract}</td>
                  <td>{earn.amount.toFixed(2)}</td>
                  <td className="positive">{earn.funding_earn.toFixed(4)}</td>
                  <td className="positive">{earn.interest_earn.toFixed(4)}</td>
                  <td className={earn.pnl >= 0 ? 'positive' : 'negative'}>
                    {earn.pnl.toFixed(2)}
                  </td>
                  <td className="positive">{earn.total_earn.toFixed(2)}</td>
                  <td>{earn.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}