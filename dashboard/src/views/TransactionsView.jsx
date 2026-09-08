import React, { useState, useEffect } from 'react';
import { ArrowLeftRight, Search, Clock, ChevronRight, X, GitCommit, Globe } from 'lucide-react';

export default function TransactionsView({ selectedTxid, onSelectTx, onCloseModal }) {
  const [txs, setTxs] = useState([]);
  const [txDetail, setTxDetail] = useState(null);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:8000/api/transactions?limit=100')
      .then(res => res.json())
      .then(data => {
        setTxs(data.transactions || []);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to load transactions:", err);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    if (selectedTxid) {
      fetch(`http://localhost:8000/api/transactions/${selectedTxid}`)
        .then(res => res.json())
        .then(data => setTxDetail(data))
        .catch(err => console.error("Failed to load tx detail:", err));
    } else {
      setTxDetail(null);
    }
  }, [selectedTxid]);

  const filteredTxs = txs.filter(t => {
    if (!search) return true;
    return t.txid.toLowerCase().includes(search.toLowerCase());
  });

  return (
    <div>
      <div className="view-header">
        <h2>Monitored Transaction Traffic ({txs.length})</h2>
        <p>Propagation timelines, fee-to-size density, whale rarity signatures, and network hop latency.</p>
      </div>

      <div className="filter-bar">
        <input
          type="text"
          className="search-input"
          placeholder="Filter by TXID hex hash..."
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>

      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>TXID</th>
              <th>Fee (BTC)</th>
              <th>Size (bytes)</th>
              <th>Rarity Score</th>
              <th>Observations</th>
              <th>Avg Delay</th>
              <th>Propagation Span</th>
              <th>Risk Score</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {filteredTxs.length === 0 ? (
              <tr>
                <td colSpan="9" style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                  {loading ? "Loading transactions..." : "No transactions matching search."}
                </td>
              </tr>
            ) : (
              filteredTxs.map(tx => (
                <tr
                  key={tx.txid}
                  className="clickable-row"
                  onClick={() => onSelectTx(tx.txid)}
                >
                  <td className="mono" style={{ fontWeight: 600, color: 'var(--accent-btc)' }}>
                    {tx.txid.substring(0, 14)}...{tx.txid.substring(tx.txid.length - 8)}
                  </td>
                  <td>
                    {parseFloat(tx.fee).toFixed(5)}
                  </td>
                  <td>
                    {tx.size.toLocaleString()} B
                  </td>
                  <td>
                    <span style={{
                      fontWeight: 600,
                      color: tx.rarity_score > 0.7 ? '#f87171' : (tx.rarity_score > 0.4 ? '#fbbf24' : '#34d399')
                    }}>
                      {parseFloat(tx.rarity_score).toFixed(3)}
                    </span>
                  </td>
                  <td>
                    {tx.total_observations} obs
                  </td>
                  <td>
                    {parseFloat(tx.avg_propagation_delay_ms).toFixed(1)} ms
                  </td>
                  <td>
                    {parseFloat(tx.observed_propagation_span_ms).toFixed(1)} ms
                  </td>
                  <td>
                    <span className={`badge ${
                      tx.risk_score >= 80 ? 'badge-critical' :
                      tx.risk_score >= 65 ? 'badge-high' :
                      tx.risk_score >= 40 ? 'badge-medium' : 'badge-low'
                    }`}>
                      {parseFloat(tx.risk_score).toFixed(1)}
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

      {/* Transaction Detail & Propagation Timeline Modal */}
      {txDetail && (
        <div style={{
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          width: '620px',
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
                <span className="mono" style={{ fontSize: '0.95rem', fontWeight: 700, color: '#fff', wordBreak: 'break-all' }}>
                  {txDetail.txid.substring(0, 20)}...
                </span>
                <span className={`badge ${
                  txDetail.risk.risk_score >= 80 ? 'badge-critical' :
                  txDetail.risk.risk_score >= 65 ? 'badge-high' :
                  txDetail.risk.risk_score >= 40 ? 'badge-medium' : 'badge-low'
                }`}>
                  Risk: {txDetail.risk.risk_score}
                </span>
              </div>
              <div style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>
                Full Hash: <span className="mono" style={{ color: 'var(--accent-cyan)' }}>{txDetail.txid}</span>
              </div>
            </div>
            <button
              onClick={onCloseModal}
              style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer' }}
            >
              <X size={20} />
            </button>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '10px' }}>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Fee</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: 'var(--accent-btc)' }}>{txDetail.fee} BTC</div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Size</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: '#fff' }}>{txDetail.size} B</div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Rarity</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: '#38bdf8' }}>{txDetail.rarity_score}</div>
            </div>
            <div style={{ background: 'var(--bg-primary)', padding: '10px', borderRadius: '6px', border: '1px solid var(--border)' }}>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Span</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: 'var(--accent-emerald)' }}>
                {txDetail.propagation_metrics.observed_propagation_span_ms} ms
              </div>
            </div>
          </div>

          <div style={{ background: 'var(--bg-primary)', padding: '12px', borderRadius: '8px', border: '1px solid var(--border)' }}>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', marginBottom: '4px' }}>AI Explanation</div>
            <div style={{ color: '#fff', fontSize: '0.88rem' }}>{txDetail.risk.explanation}</div>
          </div>

          {/* Chronological Propagation Hop Timeline */}
          <div>
            <h4 style={{ color: '#fff', fontSize: '0.95rem', marginBottom: '14px', display: 'flex', alignItems: 'center', gap: '6px' }}>
              <Clock size={16} color="var(--accent-cyan)" /> Chronological Propagation Timeline ({txDetail.timeline.length} hops)
            </h4>

            <div className="timeline">
              {txDetail.timeline.map((hop, i) => (
                <div key={hop.observation_id} className="timeline-item">
                  <div className="timeline-dot" />
                  <div className="timeline-content">
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px' }}>
                      <span style={{ fontWeight: 600, color: '#fff', fontSize: '0.85rem' }}>
                        Hop #{hop.sequence_number || (i + 1)} · {hop.message_type?.toUpperCase()} message
                      </span>
                      <span className="mono" style={{ color: 'var(--accent-emerald)', fontSize: '0.82rem', fontWeight: 600 }}>
                        +{hop.propagation_delay_ms} ms
                      </span>
                    </div>

                    <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                      <div>
                        Observer: <span className="mono" style={{ color: '#fff' }}>{hop.observer_id}</span> ({hop.observer_country})
                      </div>
                      {hop.peer_id && (
                        <div>
                          Relayed via Peer: <span className="mono" style={{ color: 'var(--accent-cyan)' }}>{hop.peer_id}</span> ({hop.peer_country})
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
