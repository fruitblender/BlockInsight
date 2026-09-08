import React, { useState, useEffect } from 'react';
import { Cpu, CheckCircle2, Sliders, Box, HardDrive } from 'lucide-react';

export default function ModelsView() {
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:8000/api/models')
      .then(res => res.json())
      .then(data => {
        setModels(data.models || []);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to load models:", err);
        setLoading(false);
      });
  }, []);

  return (
    <div>
      <div className="view-header">
        <h2>ML Model Registry & Governance (SRS §31)</h2>
        <p>Production versions, training metrics, hyperparameters, and feature schemas for offline anomaly detection models.</p>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
        {models.map(m => (
          <div key={m.model_id} className="card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '16px' }}>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '4px' }}>
                  <h3 style={{ color: '#fff', fontSize: '1.2rem' }}>{m.model_name}</h3>
                  <span className="badge" style={{ background: 'rgba(139, 92, 246, 0.15)', color: '#c084fc', border: '1px solid rgba(139, 92, 246, 0.3)' }}>
                    {m.model_version}
                  </span>
                  {m.is_production && (
                    <span className="badge badge-low" style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <CheckCircle2 size={12} /> Production
                    </span>
                  )}
                </div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  Algorithm: <span style={{ color: 'var(--accent-cyan)', fontWeight: 600 }}>{m.algorithm}</span> · Target: <span style={{ color: '#fff' }}>{m.target_entity}</span> · ID: <span className="mono">{m.model_id}</span>
                </div>
              </div>

              <div style={{ textAlign: 'right', fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                Registered: {m.created_at ? new Date(m.created_at).toLocaleDateString() : 'N/A'}
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px', marginTop: '16px' }}>
              {/* Training Metrics */}
              <div style={{ background: 'var(--bg-primary)', padding: '14px', borderRadius: '8px', border: '1px solid var(--border)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', marginBottom: '8px' }}>
                  <Sliders size={14} color="var(--accent-cyan)" /> Training Metrics
                </div>
                <pre style={{ color: 'var(--text-secondary)', fontSize: '0.8rem', fontFamily: 'JetBrains Mono', whiteSpace: 'pre-wrap' }}>
                  {JSON.stringify(m.metrics, null, 2)}
                </pre>
              </div>

              {/* Hyperparameters */}
              <div style={{ background: 'var(--bg-primary)', padding: '14px', borderRadius: '8px', border: '1px solid var(--border)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', marginBottom: '8px' }}>
                  <Box size={14} color="var(--accent-btc)" /> Hyperparameters
                </div>
                <pre style={{ color: 'var(--text-secondary)', fontSize: '0.8rem', fontFamily: 'JetBrains Mono', whiteSpace: 'pre-wrap' }}>
                  {JSON.stringify(m.hyperparameters, null, 2)}
                </pre>
              </div>

              {/* Feature Schema */}
              <div style={{ background: 'var(--bg-primary)', padding: '14px', borderRadius: '8px', border: '1px solid var(--border)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', marginBottom: '8px' }}>
                  <HardDrive size={14} color="var(--accent-emerald)" /> Feature Dimensions ({Array.isArray(m.feature_names) ? m.feature_names.length : 0})
                </div>
                <div style={{ maxHeight: '120px', overflowY: 'auto', display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                  {Array.isArray(m.feature_names) && m.feature_names.map((f, i) => (
                    <span key={i} style={{ fontSize: '0.7rem', padding: '2px 6px', background: 'rgba(255,255,255,0.05)', borderRadius: '4px', color: 'var(--text-secondary)' }}>
                      {f}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
