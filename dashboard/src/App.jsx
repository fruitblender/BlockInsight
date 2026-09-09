import { useEffect, useState } from 'react'
import { BrowserRouter, Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import { Bell, CircleHelp, Menu } from 'lucide-react'
import Sidebar from './components/Sidebar'
import { api } from './api/client'
import { Alerts, Clusters, NodeDrawer, Nodes, Operations, Overview, Models, Topology, TransactionDrawer, Transactions } from './pages'

const pageNames = { '/': 'Executive overview', '/alerts': 'Threat alerts', '/nodes': 'Node profiles', '/transactions': 'Transaction traffic', '/topology': 'Network topology', '/models': 'ML model registry', '/clusters': 'Cluster analysis', '/operations': 'Pipeline operations' }

function AppFrame() {
  const location = useLocation()
  const navigate = useNavigate()
  const [counts, setCounts] = useState(null)
  const [apiState, setApiState] = useState('checking')
  const [nodeId, setNodeId] = useState(null)
  const [txid, setTxid] = useState(null)
  const [mobileOpen, setMobileOpen] = useState(false)

  useEffect(() => {
    api.get('/api/overview').then(data => { setCounts(data); setApiState('online') }).catch(() => setApiState('offline'))
  }, [])

  const navigateTo = path => { navigate(path); setMobileOpen(false) }
  return <div className="app-shell"><Sidebar currentPath={location.pathname} navigate={navigateTo} counts={counts} mobileOpen={mobileOpen} closeMobile={() => setMobileOpen(false)} /><main className="main-content"><header className="topbar"><button className="mobile-menu icon-button" onClick={() => setMobileOpen(true)}><Menu size={18} /></button><div className="crumb"><span>BlockInsight</span><b>/</b>{pageNames[location.pathname] || 'Investigation'}</div><div className="topbar-actions"><div className={`api-indicator api-${apiState}`}><i />{apiState === 'online' ? 'API connected' : apiState === 'offline' ? 'API unavailable' : 'Checking API'}</div><button className="icon-button" aria-label="Help"><CircleHelp size={17} /></button><button className="icon-button" aria-label="Notifications"><Bell size={17} /></button></div></header><div className="content-wrap">{location.pathname === '/' ? counts ? <Overview apiOverview={counts} /> : <div className="state-block"><span>Querying live telemetry...</span></div> : <Routes><Route path="/alerts" element={<Alerts />} /><Route path="/nodes" element={<Nodes onSelectNode={setNodeId} />} /><Route path="/transactions" element={<Transactions onSelectTx={setTxid} />} /><Route path="/topology" element={<Topology onSelectNode={setNodeId} />} /><Route path="/models" element={<Models />} /><Route path="/clusters" element={<Clusters />} /><Route path="/operations" element={<Operations />} /><Route path="*" element={<Navigate to="/" replace />} /></Routes>}</div></main>{nodeId && <NodeDrawer nodeId={nodeId} onClose={() => setNodeId(null)} />}{txid && <TransactionDrawer txid={txid} onClose={() => setTxid(null)} />}</div>
}

export default function App() { return <BrowserRouter><AppFrame /></BrowserRouter> }
