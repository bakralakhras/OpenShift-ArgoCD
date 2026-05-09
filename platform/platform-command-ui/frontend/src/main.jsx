import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { Activity, Database, GitBranch, Server, ShieldAlert, Zap } from "lucide-react";
import "./style.css";

const API_URL = import.meta.env.VITE_API_URL || "/api";

function StatCard({ label, value, sub, danger }) {
  return (
    <div className={`card stat ${danger ? "danger" : ""}`}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      <div className="sub">{sub}</div>
    </div>
  );
}

function Node({ title, subtitle, active = true }) {
  return (
    <div className={`node ${active ? "active" : "warn"}`}>
      <div className="dot" />
      <div>
        <strong>{title}</strong>
        <span>{subtitle}</span>
      </div>
    </div>
  );
}

function App() {
  const [data, setData] = useState(null);

  useEffect(() => {
    const load = async () => {
      const res = await fetch(`${API_URL}/overview`);
      setData(await res.json());
    };

    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  const p = data?.platform;
  const f = data?.fraud;

  return (
    <main>
      <header>
        <div>
          <h1>Sovereign Command Center</h1>
          <p>OpenShift data platform observability interface</p>
        </div>
        <div className="live">LIVE TELEMETRY</div>
      </header>

      <section className="grid top">
        <div className="card hero">
          <div className="heroTop">
            <Activity />
            <span>Platform Health</span>
          </div>
          <div className="score">{p ? `${p.health_score}%` : "--"}</div>
          <div className="bar">
            <div style={{ width: `${p?.health_score || 0}%` }} />
          </div>
        </div>

        <StatCard label="Nodes Ready" value={p?.nodes_ready ?? "--"} sub="OpenShift workers/masters" />
        <StatCard label="Bad Pods" value={p?.bad_pods ?? "--"} sub="Non-running workloads" danger={(p?.bad_pods || 0) > 50} />
        <StatCard label="Argo Synced" value={p?.argo_synced ?? "--"} sub="GitOps applications" />
        <StatCard label="Targets Up" value={p?.targets_up ?? "--"} sub="Prometheus scrape health" />
      </section>

      <section className="grid middle">
        <div className="card topology">
          <h2>Live Platform Flow</h2>
          <div className="flow">
            <Node title="Ingress" subtitle="Entry layer" />
            <Zap className="arrow" />
            <Node title="Spark Batch" subtitle="Bronze → Silver" />
            <Zap className="arrow" />
            <Node title="Kafka" subtitle="Event bus" />
            <Zap className="arrow" />
            <Node title="Spark" subtitle="Fraud engine" />
            <Zap className="arrow" />
            <Node title="MinIO" subtitle="Iceberg lake" />
            <Zap className="arrow" />
            <Node title="Trino" subtitle="Analytics SQL" />
          </div>
        </div>

        <div className="card ops">
          <h2>Operational Load</h2>
          <div className="metric">
            <span>CPU</span>
            <strong>{p ? `${Math.round(p.cpu * 100)}%` : "--"}</strong>
          </div>
          <div className="thin"><div style={{ width: `${(p?.cpu || 0) * 100}%` }} /></div>

          <div className="metric">
            <span>Memory</span>
            <strong>{p ? `${Math.round(p.memory * 100)}%` : "--"}</strong>
          </div>
          <div className="thin"><div style={{ width: `${(p?.memory || 0) * 100}%` }} /></div>
        </div>
      </section>

      <section className="grid bottom">
        <StatCard label="Transactions" value={f?.transactions?.toLocaleString() ?? "--"} sub="Protected volume" />
        <StatCard label="Fraud Cases" value={f?.fraud_cases?.toLocaleString() ?? "--"} sub="Detected risk events" danger />
        <StatCard label="Fraud Rate" value={f ? `${(f.fraud_rate * 100).toFixed(2)}%` : "--"} sub="Business risk signal" />
        <StatCard label="Decisions" value={f?.decisions?.toLocaleString() ?? "--"} sub="Silver layer decisions" />
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
