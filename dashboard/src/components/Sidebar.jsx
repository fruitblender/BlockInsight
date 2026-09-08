import React from 'react';
import { Activity, ShieldAlert, Network, ArrowLeftRight, GitBranch, Cpu, Database } from 'lucide-react';

export default function Sidebar({ activeView, setActiveView, counts }) {
  const navItems = [
    { id: 'overview', label: 'Executive Overview', icon: Activity },
    { id: 'alerts', label: 'Threat Alerts', icon: ShieldAlert, badge: counts?.alerts || 0 },
    { id: 'nodes', label: 'Node Profiles', icon: Network, badge: counts?.nodes || 150 },
    { id: 'transactions', label: 'Transaction Traffic', icon: ArrowLeftRight, badge: counts?.txs || 500 },
    { id: 'graph', label: 'Network Link Graph', icon: GitBranch },
    { id: 'models', label: 'Model Registry', icon: Cpu },
  ];

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <div className="logo-badge">₿</div>
        <div className="logo-text">
          <h1>ChainWatch</h1>
          <p>BTC Traffic Monitor</p>
        </div>
      </div>

      <div className="nav-links">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = activeView === item.id;
          return (
            <button
              key={item.id}
              className={`nav-item ${isActive ? 'active' : ''}`}
              onClick={() => setActiveView(item.id)}
            >
              <Icon size={18} />
              <span style={{ flex: 1, textAlign: 'left' }}>{item.label}</span>
              {item.badge !== undefined && (
                <span style={{
                  fontSize: '0.72rem',
                  padding: '2px 6px',
                  borderRadius: '10px',
                  background: isActive ? 'rgba(6, 182, 212, 0.25)' : 'rgba(255, 255, 255, 0.08)',
                  color: isActive ? '#38bdf8' : '#94a3b8',
                  fontWeight: 600
                }}>
                  {item.badge}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div className="sidebar-footer">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
          <span>Offline Node</span>
          <span className="status-pill">
            <span className="status-dot"></span> Online
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <Database size={13} color="#64748b" />
          <span>PostgreSQL · Batch #1</span>
        </div>
      </div>
    </aside>
  );
}
