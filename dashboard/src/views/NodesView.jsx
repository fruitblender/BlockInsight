import React, { useState, useEffect } from 'react';
import { Network, Search, Filter, ChevronRight, X, GitCommit, Layers } from 'lucide-react';

export default function NodesView({ selectedNodeId, onSelectNode, onCloseModal }) {
  const [nodes, setNodes] = useState([]);
  const [nodeDetail, setNodeDetail] = useState(null);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:8000/api/nodes?limit=150')
      .then(res => res.json())
      .then(data => {
        setNodes(data.nodes || []);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to load nodes:", err);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    if (selectedNodeId) {
      fetch(`http://localhost:8000/api/nodes/${selectedNodeId}`)
        .then(res => res.json())
        .then(data => setNodeDetail(data))
        .catch(err => console.error("Failed to load node detail:", err));
    } else {
      setNodeDetail(null);
    }
  }, [selectedNodeId]);

  const filteredNodes = nodes.filter(n => {
    if (!search) return true;
    const term = search.toLowerCase();
    return (
      n.node_id.toLowerCase().includes(term) ||
      n.ip.toLowerCase().includes(term) ||
      (n.country && n.country.toLowerCase().includes(term)) ||
      (n.node_type && n.node_type.toLowerCase().includes(term))
    );
  });

  return (
    <div>
      <div className="view-header">
        <h2>Monitored P2P Nodes ({nodes.length})</h2>
        <p>Topological centrality, behavioral observation metrics, DBSCAN clusters, and risk profiling.</p>
      </div>

      <div className="filter-bar">
        <input
          type="text"
          className="search-input"
          placeholder="Filter by Node ID, IP address, country, or type..."
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>

      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Node ID</th>
              <th>IP Endpoint</th>
              <th>Country</th>
              <th>Node Type</th>
              <th>Degree / Conns</th>
              <th>DBSCAN Cluster</th>
              <th>Community</th>
              <th>Avg Delay</th>
              <th>Risk Score</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {filteredNodes.length === 0 ? (
              <tr>
                <td colSpan="10" style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                  {loading ? "Loading node telemetry..." : "No nodes matching search."}
                </td>
              </tr>
            ) : (
              filteredNodes.map(node => (
                <tr
                  key={node.node_id}
                  className="clickable-row"
                  onClick={() => onSelectNode(node.node_id)}
                >
                  <td className="mono" style={{ fontWeight: 600, color: '#fff' }}>
                    {node.node_id}
                  </td>
                  <td className="mono" style={{ color: 'var(--accent-cyan)' }}>
                    {node.ip}
                  </td>
                  <td>
                    <span style={{ fontWeight: 600 }}>{node.country || 'N/A'}</span>
                  </td>
                  <td style={{ fontSize: '0.82rem', color: 'var(--text-secondary)' }}>
                    {node.node_type}
                  </td>
                  <td>
                    {node.peer_degree} <span style={{ color: 'var(--text-muted)' }}>({node.total_connections} edges)</span>
                  </td>
                  <td>
                    <span style={{
                      padding: '2px 6px',
                      borderRadius: '4px',
                      fontSize: '0.75rem',
                      fontWeight: 600,
                      background: node.cluster_id === -1 ? 'rgba(239, 68, 68, 0.15)' : 'rgba(139, 92, 246, 0.15)',
                      color: node.cluster_id === -1 ? '#f87171' : '#c084fc',
                      border: `1px solid ${node.cluster_id === -1 ? 'rgba(239,68,68,0.3)' : 'rgba(139,92,246,0.3)'}`
                    }}>
                      {node.cluster_id === -1 ? 'Noise (-1)' : `Cluster #${node.cluster_id}`}
                    </span>
                  </td>
                  <td>
                    <span style={{ color: 'var(--text-secondary)' }}>#{node.community_id}</span>
                  </td>
                  <td>
                    {node.avg_observer_delay_ms > 0 ? `${node.avg_observer_delay_ms.toFixed(1)} ms` : '-'}
                  </td>
                  <td>
                    <span className={`badge ${
                      node.risk_score >= 80 ? 'badge-critical' :
                      node.risk_score >= 65 ? 'badge-high' :
                      node.risk_score >= 40 ? 'badge-medium' : 'badge-low'
                    }`}>
                      {node.risk_score.toFixed(1)}
                    </span>
                  </td>
                  <td>
                    <button style={{ background: 'transparent', border: 'none', color: 'var(--accent-cyan)', cursor: 'pointer' }}>
                      <ChevronRight size={16} />
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Node Detail Modal */}
      {nodeDetail && (
        <div style={{
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          width: '580px',
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
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
                <span className="mono" style={{ fontSize: '1.2rem', fontWeight: 700, color: '#fff' }}>
                  {nodeDetail.node_id}
                </span>
                <span className={`badge ${
                  nodeDetail.risk.risk_score >= 80 ? 'badge-critical' :
                  nodeDetail.risk.risk_score >= 65 ? 'badge-high' :
                  nodeDetail.risk.risk_score >= 40 ? 'badge-medium' : 'badge-low'
                }`}>
                  Risk: {nodeDetail.risk.risk_score}
                </span>
              </div>
              <div style={{ color: 'var(--accent-cyan)', fontSize: '0.85rem' }} className="mono">
                {nodeDetail.ip} · {nodeDetail.country} · {nodeDetail.asn}
              </div>
            </div>
            <button
              onClick={onCloseModal}
              style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer' }}
            >
              <X size={20} />
            </button>
          </div>

          <div style={{ background: 'var(--bg-primary)', padding: '14px', borderRadius: '8px', border: '1px solid var(--border)' }}>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', marginBottom: '4px' }}>AI Explanation</div>
            <div style={{ color: '#fff', fontSize: '0.88rem' }}>{nodeDetail.risk.explanation}</div>
          </div>

          {/* Topology & Behavior KPI Grid */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '10px' }}>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Degree Centrality</div>
              <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>{nodeDetail.topology.degree_centrality.toFixed(4)}</div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Betweenness</div>
              <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>{nodeDetail.topology.betweenness_centrality.toFixed(4)}</div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Closeness</div>
              <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>{nodeDetail.topology.closeness_centrality.toFixed(4)}</div>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '10px' }}>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Observed Txs</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700, color: 'var(--accent-cyan)' }}>{nodeDetail.behavior.transactions_observed}</div>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{nodeDetail.behavior.observations_as_observer} total events</div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Propagated Txs</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700, color: 'var(--accent-btc)' }}>{nodeDetail.behavior.transactions_propagated}</div>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{nodeDetail.behavior.propagations_as_peer} peer relays</div>
            </div>
          </div>

          {/* Connected Peers List */}
          <div>
            <h4 style={{ color: '#fff', fontSize: '0.92rem', marginBottom: '10px' }}>
              Connected Peer Neighbors ({nodeDetail.neighbors.length})
            </h4>
            <div style={{ maxHeight: '200px', overflowY: 'auto', border: '1px solid var(--border)', borderRadius: '6px' }}>
              <table>
                <thead>
                  <tr>
                    <th>Peer ID</th>
                    <th>IP</th>
                    <th>Direction</th>
                  </tr>
                </thead>
                <tbody>
                  {nodeDetail.neighbors.map((nb, i) => (
                    <tr key={i}>
                      <td className="mono" style={{ color: '#fff' }}>{nb.peer_id}</td>
                      <td className="mono" style={{ color: 'var(--accent-cyan)' }}>{nb.peer_ip}</td>
                      <td style={{ fontSize: '0.75rem', color: nb.direction === 'OUTGOING' ? '#38bdf8' : '#34d399' }}>
                        {nb.direction}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
