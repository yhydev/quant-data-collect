import { useEffect } from 'react'
import { NavLink, Navigate, Route, Routes, useNavigate, useParams } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import FundingRates from './pages/FundingRates'
import OpenPosition from './pages/OpenPosition'
import OpenProgress from './pages/OpenProgress'
import ClosePosition from './pages/ClosePosition'
import CloseProgress from './pages/CloseProgress'

function OpenProgressRoute() {
  const params = useParams()
  const id = params.positionId ? Number(params.positionId) : undefined
  const positionId = Number.isFinite(id) ? id : undefined
  return <OpenProgress positionId={positionId} />
}

function CloseProgressRoute() {
  const params = useParams()
  const id = params.positionId ? Number(params.positionId) : undefined
  const positionId = Number.isFinite(id) ? id : undefined
  return <CloseProgress positionId={positionId} />
}

export default function App() {
  const navigate = useNavigate()

  useEffect(() => {
    const rawOpen = localStorage.getItem('openProgressPositionId')
    const rawClose = localStorage.getItem('closeProgressPositionId')
    if (rawOpen && window.location.pathname === '/open-progress') {
      navigate(`/open-progress/${rawOpen}`, { replace: true })
    }
    if (rawClose && window.location.pathname === '/close-progress') {
      navigate(`/close-progress/${rawClose}`, { replace: true })
    }
  }, [navigate])

  return (
    <div>
      <nav className="top-nav">
        <NavLink to="/dashboard">Dashboard</NavLink>
        <NavLink to="/funding">Funding</NavLink>
        <NavLink to="/open">Open</NavLink>
        <NavLink to="/open-progress">Open Progress</NavLink>
        <NavLink to="/close">Close</NavLink>
        <NavLink to="/close-progress">Close Progress</NavLink>
      </nav>

      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/funding" element={<FundingRates />} />
        <Route
          path="/open"
          element={
            <OpenPosition
              onSuccess={(positionId) => {
                localStorage.setItem('openProgressPositionId', String(positionId))
                navigate(`/open-progress/${positionId}`)
              }}
            />
          }
        />
        <Route path="/open-progress" element={<OpenProgress />} />
        <Route path="/open-progress/:positionId" element={<OpenProgressRoute />} />
        <Route
          path="/close"
          element={
            <ClosePosition
              onSuccess={(positionId) => {
                localStorage.setItem('closeProgressPositionId', String(positionId))
                navigate(`/close-progress/${positionId}`)
              }}
            />
          }
        />
        <Route path="/close-progress" element={<CloseProgress />} />
        <Route path="/close-progress/:positionId" element={<CloseProgressRoute />} />
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </div>
  )
}
