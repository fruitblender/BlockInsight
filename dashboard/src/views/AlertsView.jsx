import React, { useState, useEffect } from 'react';
import { ShieldAlert, Search, Filter, ChevronRight, X, Info } from 'lucide-react';

export default function AlertsView({ onSelectAlert, selectedAlert, onCloseDetail }) {
  const [alerts, setAlerts] = useState([]);
  const [severityFilter, setSeverityFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);

  const fetchAlerts = () => {
    setLoading(true);
    let url = 'http://localhost:8000/api/alerts?limit=100';
    if (severityFilter) url += `&severity=${severityFilter}`;
    if (typeFilter) url += `&entity_type=${typeFilter}`;

    fetch(url)
      .then(res => res.json())
      .then(data => {
        setAlerts(data.alerts || []);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to load alerts:", err);
        setLoading(false);
      });
  };

  useEffect(() => {
    fetchAlerts();
  }, [severityFilter, typeFilter]);

  const filteredAlerts = alerts.filter(a => {
    if (!search) return true;
    const term = search.toLowerCase();
    return (
      a.entity_id.toLowerCase().includes(term) ||
      a.title.toLowerCase().includes(term) ||
      a.description.toLowerCase().includes(term)
    );
  });

  return (
    <div>
      <div className="view-header">
        <h2>Threat Alert Investigation Center</h2>
        <p>Ranked, explainable anomalies detected across Bitcoin network topology and transaction propagation.</p>
      </div>

      {/* Filter Bar */}
      <div className="filter-bar">
        <div style={{ position: 'relative' }}>
          <input
            type="text"
            className="search-input"
            placeholder="Search by Entity ID, title, or reason..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>

        <select
          className="select-input"
          value={severityFilter}
          onChange={e => setSeverityFilter(e.target.value)}
        >
          <option value="">All Severities</option>
          <option value="CRITICAL">Critical Only (&gt;= 80)</option>
          <option value="HIGH">High Only (65 - 79)</option>
        </select>

        <select
          className="select-input"
          value={typeFilter}
          onChange={e => setTypeFilter(e.target.value)}
        >
          <option value="">All Entity Types</option>
          <option value="NODE">Nodes (P2P)</option>
          <option value="TRANSACTION">Transactions (TXID)</option>
        </select>
      </div>

      {/* Alerts Table */}
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Severity</th>
              <th>Type</th>
              <th>Entity ID</th>
              <th>Risk Score</th>
              <th>Confidence</th>
              <th>Description & AI Rationale</th>
              <th>Status</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {filteredAlerts.length === 0 ? (
              <tr>
                <td colSpan="8" style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                  {loading ? "Loading alerts..." : "No alerts matching current filters."}
                </td>
              </tr>
            ) : (
              filteredAlerts.map(alert => (
                <tr
                  key={alert.alert_id}
                  className="clickable-row"
                  onClick={() => onSelectAlert(alert)}
                >
                  <td>
                    <span className={`badge ${alert.severity === 'CRITICAL' ? 'badge-critical' : 'badge-high'}`}>
                      {alert.severity}
                    </span>
                  </td>
                  <td>
                    <span className={`badge ${alert.entity_type === 'NODE' ? 'badge-node' : 'badge-tx'}`}>
                      {alert.entity_type}
                    </span>
                  </td>
                  <td className="mono" style={{ fontWeight: 600, color: '#fff' }}>
                    {alert.entity_id.length > 20 ? alert.entity_id.substring(0, 16) + '...' : alert.entity_id}
                  </td>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{
                        fontWeight: 700,
                        color: alert.risk_score >= 80 ? '#ef4444' : '#f97316'
                      }}>
                        {alert.risk_score}
                      </span>
                      <div style={{
                        width: '45px',
                        height: '6px',
                        background: 'rgba(255,255,255,0.1)',
                        borderRadius: '3px',
                        overflow: 'hidden'
                      }}>
                        <div style={{
                          width: `${alert.risk_score}%`,
                          height: '100%',
                          background: alert.risk_score >= 80 ? '#ef4444' : '#f97316'
                        }} />
                      </div>
                    </div>
                  </td>
                  <td style={{ color: 'var(--text-secondary)' }}>
                    {(alert.confidence * 100).toFixed(0)}%
                  </td>
                  <td style={{ maxWidth: '420px', fontSize: '0.84rem' }}>
                    <div style={{ color: '#fff', fontWeight: 500, marginBottom: '2px' }}>{alert.title}</div>
                    <div style={{ color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {alert.description}
                    </div>
                  </td>
                  <td>
                    <span style={{
                      display: 'inline-block',
                      padding: '2px 8px',
                      borderRadius: '4px',
                      background: 'rgba(6, 182, 212, 0.1)',
                      color: 'var(--accent-cyan)',
                      fontSize: '0.72rem',
                      fontWeight: 600
                    }}>
                      {alert.status}
                    </span>
                  </td>
                  <td>
                    <button
                      style={{
                        background: 'transparent',
                        border: 'none',
                        color: 'var(--accent-cyan)',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '4px',
                        fontSize: '0.82rem'
                      }}
                    >
                      Inspect <ChevronRight size={14} />
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Detail Modal */}
      {selectedAlert && (
        <div style={{
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          width: '540px',
          background: 'var(--bg-secondary)',
          borderLeft: '1px solid var(--border)',
          boxShadow: 'var(--shadow-lg)',
          zIndex: 100,
          padding: '28px',
          display: 'flex',
          flexDirection: 'column',
          gap: '20px',
          overflowY: 'auto'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className={`badge ${selectedAlert.severity === 'CRITICAL' ? 'badge-critical' : 'badge-high'}`}>
                {selectedAlert.severity}
              </span>
              <span className={`badge ${selectedAlert.entity_type === 'NODE' ? 'badge-node' : 'badge-tx'}`}>
                {selectedAlert.entity_type}
              </span>
            </div>
            <button
              onClick={onCloseDetail}
              style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer' }}
            >
              <X size={20} />
            </button>
          </div>

          <div>
            <h3 style={{ color: '#fff', fontSize: '1.25rem', marginBottom: '8px' }}>{selectedAlert.title}</h3>
            <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem' }}>{selectedAlert.description}</p>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
            <div style={{ background: 'var(--bg-primary)', padding: '12px', borderRadius: '8px', border: '1px solid var(--border)' }}>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase' }}>Risk Score</div>
              <div style={{ fontSize: '1.5rem', fontWeight: 700, color: selectedAlert.risk_score >= 80 ? '#ef4444' : '#f97316' }}>
                {selectedAlert.risk_score} / 100
              </div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '12px', borderRadius: '8px', border: '1px solid var(--border)' }}>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase' }}>AI Confidence</div>
              <div style={{ fontSize: '1.5rem', fontWeight: 700, color: 'var(--accent-emerald)' }}>
                {(selectedAlert.confidence * 100).toFixed(1)}%
              </div>
            </div>
          </div>

          {/* Evidence Details */}
          <div>
            <h4 style={{ color: '#fff', fontSize: '0.95rem', marginBottom: '10px', display: 'flex', alignItems: 'center', gap: '6px' }}>
              <Info size={16} color="var(--accent-cyan)" /> Evidence & Wire Signatures
            </h4>
            <div style={{ background: 'var(--bg-primary)', padding: '16px', borderRadius: '8px', border: '1px solid var(--border)', fontSize: '0.85rem' }}>
              <pre style={{ color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', wordBreak: 'break-all', fontFamily: 'JetBrains Mono' }}>
                {JSON.stringify(selectedAlert.evidence, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
