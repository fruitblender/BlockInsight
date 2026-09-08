import React, { useState, useEffect, useRef } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { GitBranch, Layers, ShieldAlert, ZoomIn, ZoomOut, RefreshCw } from 'lucide-react';

export default function GraphView({ onSelectNode }) {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [colorMode, setColorMode] = useState('risk'); // 'risk' or 'community'
  const [hoverNode, setHoverNode] = useState(null);
  const [loading, setLoading] = useState(true);
  const fgRef = useRef();

  useEffect(() => {
    fetch('http://localhost:8000/api/graph?limit_nodes=150')
      .then(res => res.json())
      .then(data => {
        setGraphData(data);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to load graph data:", err);
        setLoading(false);
      });
  }, []);

  const getNodeColor = (node) => {
    if (colorMode === 'risk') {
      if (node.risk_score >= 80) return '#ef4444';
      if (node.risk_score >= 65) return '#f97316';
      if (node.risk_score >= 40) return '#eab308';
      return '#10b981';
    } else {
      // Community color palette
      const colors = ['#06b6d4', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#3b82f6', '#14b8a6', '#f43f5e'];
      return colors[(node.community_id || 0) % colors.length];
    }
  };

  return (
    <div>
      <div className="view-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <h2>Network Link & Topology Graph Analysis</h2>
          <p>Interactive force-directed visualization of 150 Bitcoin P2P nodes and 441 peer connection channels.</p>
        </div>

        <div style={{ display: 'flex', gap: '10px' }}>
          <div style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: '8px', padding: '4px' }}>
            <button
              onClick={() => setColorMode('risk')}
              style={{
                background: colorMode === 'risk' ? 'rgba(239, 68, 68, 0.2)' : 'transparent',
                color: colorMode === 'risk' ? '#f87171' : 'var(--text-secondary)',
                border: 'none',
                borderRadius: '6px',
                padding: '6px 12px',
                fontSize: '0.8rem',
                cursor: 'pointer',
                fontWeight: 600
              }}
            >
              Risk Level
            </button>
            <button
              onClick={() => setColorMode('community')}
              style={{
                background: colorMode === 'community' ? 'rgba(6, 182, 212, 0.2)' : 'transparent',
                color: colorMode === 'community' ? 'var(--accent-cyan)' : 'var(--text-secondary)',
                border: 'none',
                borderRadius: '6px',
                padding: '6px 12px',
                fontSize: '0.8rem',
                cursor: 'pointer',
                fontWeight: 600
              }}
            >
              Community Partition
            </button>
          </div>
        </div>
      </div>

      <div className="card" style={{ padding: 0, overflow: 'hidden', position: 'relative' }}>
        {/* Legend Overlay */}
        <div style={{
          position: 'absolute',
          top: '16px',
          left: '16px',
          background: 'rgba(16, 21, 34, 0.85)',
          backdropFilter: 'blur(12px)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '12px 16px',
          zIndex: 10,
          fontSize: '0.78rem'
        }}>
          <div style={{ fontWeight: 600, color: '#fff', marginBottom: '8px' }}>
            {colorMode === 'risk' ? 'Risk Severity' : 'Louvain Communities'}
          </div>
          {colorMode === 'risk' ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#ef4444' }} />
                <span>Critical Risk (&gt;= 80)</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#f97316' }} />
                <span>High Risk (65 - 79)</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#eab308' }} />
                <span>Medium Risk (40 - 64)</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#10b981' }} />
                <span>Low Risk (&lt; 40)</span>
              </div>
            </div>
          ) : (
            <div style={{ color: 'var(--text-secondary)' }}>8 distinct topological communities</div>
          )}
        </div>

        {/* Hover Inspector Pill */}
        {hoverNode && (
          <div style={{
            position: 'absolute',
            bottom: '16px',
            left: '16px',
            background: 'rgba(16, 21, 34, 0.9)',
            backdropFilter: 'blur(12px)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            padding: '10px 16px',
            zIndex: 10,
            fontSize: '0.82rem'
          }}>
            <span style={{ color: '#fff', fontWeight: 600 }}>{hoverNode.id}</span>
            <span style={{ color: 'var(--text-muted)' }}> · </span>
            <span className="mono" style={{ color: 'var(--accent-cyan)' }}>{hoverNode.ip}</span>
            <span style={{ color: 'var(--text-muted)' }}> · </span>
            <span style={{ color: hoverNode.risk_score >= 80 ? '#ef4444' : '#f97316', fontWeight: 600 }}>
              Risk: {hoverNode.risk_score}
            </span>
          </div>
        )}

        {/* Force Graph Canvas */}
        <div style={{ width: '100%', height: '640px', background: '#090d16' }}>
          {loading ? (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)' }}>
              Loading interactive graph topology...
            </div>
          ) : (
            <ForceGraph2D
              ref={fgRef}
              graphData={graphData}
              nodeId="id"
              nodeLabel="name"
              nodeVal="val"
              nodeColor={getNodeColor}
              linkColor={() => 'rgba(255, 255, 255, 0.12)'}
              linkWidth={1.2}
              backgroundColor="#090d16"
              onNodeHover={node => setHoverNode(node || null)}
              onNodeClick={node => onSelectNode(node.id)}
              cooldownTicks={100}
              onEngineStop={() => fgRef.current?.zoomToFit(400, 30)}
            />
          )}
        </div>
      </div>
    </div>
  );
}
