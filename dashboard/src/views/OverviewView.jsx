import React, { useEffect, useState } from 'react';
import { ShieldAlert, Network, ArrowLeftRight, Clock, Activity, AlertTriangle, ExternalLink } from 'lucide-react';

export default function OverviewView({ onSelectAlert, onSelectNode, onSelectTx }) {
  const [overview, setOverview] = useState(null);
  const [recentAlerts, setRecentAlerts] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:8000/api/overview')
      .then(res => res.json())
      .then(data => setOverview(data))
      .catch(err => console.error("Error loading overview:", err));

    fetch('http://localhost:8000/api/alerts?limit=5')
      .then(res => res.json())
      .then(data => {
        setRecentAlerts(data.alerts || []);
        setLoading(false);
      })
      .catch(err => {
        console.error("Error loading alerts:", err);
        setLoading(false);
      });
  }, []);

  if (loading || !overview) {
    return (
      <div style={{ padding: '60px', textAlign: 'center', color: 'var(--text-secondary)' }}>
        <Activity className="animate-spin" size={32} color="var(--accent-cyan)" />
        <p style={{ marginTop: '16px' }}>Loading real-time network intelligence...</p>
      </div>
    );
  }

  return (
    <div>
      <div className="view-header">
        <h2>Executive Monitoring Overview</h2>
        <p>Real-time P2P Bitcoin network telemetry, propagation latency, and AI behavioral threat alerts.</p>
      </div>

      {/* KPI Cards Grid */}
      <div className="grid-4">
        <div className="card kpi-card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span className="kpi-title">Monitored Nodes</span>
            <Network size={18} color="var(--accent-cyan)" />
          </div>
          <div className="kpi-value">{overview.network.total_nodes}</div>
          <div className="kpi-sub">{overview.network.communities_count} topological communities</div>
        </div>

        <div className="card kpi-card btc">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span className="kpi-title">Transactions Monitored</span>
            <ArrowLeftRight size={18} color="var(--accent-btc)" />
          </div>
          <div className="kpi-value">{overview.network.total_transactions}</div>
          <div className="kpi-sub">{overview.network.total_observations.toLocaleString()} wire observations</div>
        </div>

        <div className="card kpi-card critical">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span className="kpi-title">Threat Alerts</span>
            <ShieldAlert size={18} color="var(--risk-critical)" />
          </div>
          <div className="kpi-value" style={{ color: '#f87171' }}>{overview.alerts.total}</div>
          <div className="kpi-sub">
            <span style={{ color: '#ef4444', fontWeight: 600 }}>{overview.alerts.critical} Critical</span> · {overview.alerts.high} High
          </div>
        </div>

        <div className="card kpi-card emerald">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span className="kpi-title">Avg Propagation Delay</span>
            <Clock size={18} color="var(--accent-emerald)" />
          </div>
          <div className="kpi-value">{overview.propagation.avg_propagation_delay_ms} <span style={{ fontSize: '1.1rem' }}>ms</span></div>
          <div className="kpi-sub">Span: {overview.propagation.avg_propagation_span_ms} ms network-wide</div>
        </div>
      </div>

      {/* Two Column Layout: Cluster Health + Recent Alerts */}
      <div className="grid-2">
        {/* Network Health & AI Clustering */}
        <div className="card">
          <h3 style={{ fontSize: '1.1rem', marginBottom: '16px', color: '#fff' }}>Network Health & Topography</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '12px', background: 'rgba(255,255,255,0.03)', borderRadius: '8px' }}>
              <span style={{ color: 'var(--text-secondary)' }}>DBSCAN Behavioral Clusters:</span>
              <span style={{ fontWeight: 600, color: 'var(--accent-cyan)' }}>{overview.network.dbscan_clusters_count} Dense Clusters</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '12px', background: 'rgba(255,255,255,0.03)', borderRadius: '8px' }}>
              <span style={{ color: 'var(--text-secondary)' }}>Network Community Modularity:</span>
              <span style={{ fontWeight: 600, color: 'var(--accent-emerald)' }}>{overview.network.communities_count} Louvain Partitions</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '12px', background: 'rgba(255,255,255,0.03)', borderRadius: '8px' }}>
              <span style={{ color: 'var(--text-secondary)' }}>Anomalous Peer Rate:</span>
              <span style={{ fontWeight: 600, color: '#fb923c' }}>
                {((overview.alerts.node_alerts / overview.network.total_nodes) * 100).toFixed(1)}% of nodes flagged
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '12px', background: 'rgba(255,255,255,0.03)', borderRadius: '8px' }}>
              <span style={{ color: 'var(--text-secondary)' }}>Database Pipeline State:</span>
              <span style={{ fontWeight: 600, color: 'var(--accent-emerald)' }}>Synchronized (Stage A-E + Analytics)</span>
            </div>
          </div>
        </div>

        {/* High Priority Alerts Preview */}
        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
            <h3 style={{ fontSize: '1.1rem', color: '#fff' }}>Top Ranked Threat Alerts</h3>
            <span className="badge badge-critical">{overview.alerts.total} Active</span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {recentAlerts.map(alert => (
              <div
                key={alert.alert_id}
                onClick={() => onSelectAlert(alert)}
                style={{
                  padding: '12px',
                  background: 'rgba(255, 255, 255, 0.02)',
                  border: '1px solid var(--border)',
                  borderRadius: '8px',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center'
                }}
                onMouseEnter={e => e.currentTarget.style.borderColor = 'rgba(6, 182, 212, 0.4)'}
                onMouseLeave={e => e.currentTarget.style.borderColor = 'var(--border)'}
              >
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
                    <span className={`badge ${alert.severity === 'CRITICAL' ? 'badge-critical' : 'badge-high'}`}>
                      {alert.severity}
                    </span>
                    <span className={`badge ${alert.entity_type === 'NODE' ? 'badge-node' : 'badge-tx'}`}>
                      {alert.entity_type}
                    </span>
                    <span style={{ fontWeight: 600, color: '#fff', fontSize: '0.88rem' }}>
                      {alert.entity_id.length > 20 ? alert.entity_id.substring(0, 16) + '...' : alert.entity_id}
                    </span>
                  </div>
                  <div style={{ color: 'var(--text-secondary)', fontSize: '0.8rem', maxWidth: '380px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {alert.description}
                  </div>
                </div>

                <div style={{ textAlign: 'right' }}>
                  <div style={{ color: '#f87171', fontWeight: 700, fontSize: '1.1rem' }}>
                    {alert.risk_score}
                  </div>
                  <div style={{ color: 'var(--text-muted)', fontSize: '0.72rem' }}>Risk Score</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
