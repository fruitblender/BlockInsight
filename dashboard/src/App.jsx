import React, { useState, useEffect } from 'react';
import Sidebar from './components/Sidebar';
import OverviewView from './views/OverviewView';
import AlertsView from './views/AlertsView';
import NodesView from './views/NodesView';
import TransactionsView from './views/TransactionsView';
import GraphView from './views/GraphView';
import ModelsView from './views/ModelsView';

export default function App() {
  const [activeView, setActiveView] = useState('overview');
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [selectedTxid, setSelectedTxid] = useState(null);
  const [counts, setCounts] = useState({ alerts: 8, nodes: 150, txs: 500 });

  useEffect(() => {
    fetch('http://localhost:8000/api/overview')
      .then(res => res.json())
      .then(data => {
        if (data) {
          setCounts({
            alerts: data.alerts?.total || 8,
            nodes: data.network?.total_nodes || 150,
            txs: data.network?.total_transactions || 500
          });
        }
      })
      .catch(err => console.error("Error fetching header counts:", err));
  }, []);

  const handleSelectAlert = (alert) => {
    setSelectedAlert(alert);
    if (activeView !== 'alerts') {
      setActiveView('alerts');
    }
  };

  const handleSelectNode = (nodeId) => {
    setSelectedNodeId(nodeId);
    if (activeView !== 'nodes') {
      setActiveView('nodes');
    }
  };

  const handleSelectTx = (txid) => {
    setSelectedTxid(txid);
    if (activeView !== 'transactions') {
      setActiveView('transactions');
    }
  };

  return (
    <div className="app-container">
      <Sidebar
        activeView={activeView}
        setActiveView={setActiveView}
        counts={counts}
      />

      <main className="main-content">
        {activeView === 'overview' && (
          <OverviewView
            onSelectAlert={handleSelectAlert}
            onSelectNode={handleSelectNode}
            onSelectTx={handleSelectTx}
          />
        )}

        {activeView === 'alerts' && (
          <AlertsView
            selectedAlert={selectedAlert}
            onSelectAlert={setSelectedAlert}
            onCloseDetail={() => setSelectedAlert(null)}
          />
        )}

        {activeView === 'nodes' && (
          <NodesView
            selectedNodeId={selectedNodeId}
            onSelectNode={setSelectedNodeId}
            onCloseModal={() => setSelectedNodeId(null)}
          />
        )}

        {activeView === 'transactions' && (
          <TransactionsView
            selectedTxid={selectedTxid}
            onSelectTx={setSelectedTxid}
            onCloseModal={() => setSelectedTxid(null)}
          />
        )}

        {activeView === 'graph' && (
          <GraphView
            onSelectNode={handleSelectNode}
          />
        )}

        {activeView === 'models' && (
          <ModelsView />
        )}
      </main>
    </div>
  );
}
