import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Database,
  GitBranch,
  Server,
  ShieldAlert,
  Layers3,
  Workflow,
  Zap,
  Compass,
} from "lucide-react";

import ServiceCatalog from "./ServiceCatalog";
import "./style.css";

const API_URL = import.meta.env.VITE_API_URL || "/api";

const views = [
  "Overview",
  "Infrastructure",
  "GitOps",
  "Data Platform",
  "Platform Access",
  "Incidents",
];

function StatCard({ label, value, sub, danger }) {
  return (
    <div className={`card stat ${danger ? "danger" : ""}`}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      <div className="sub">{sub}</div>
    </div>
  );
}

function Sidebar({ active, setActive }) {
  return (
    <aside className="sidebar">
      <div className="sidebarLogo">
        <Layers3 size={18} />
        <span>SOVEREIGN</span>
      </div>

      <div className="sidebarMenu">
        {views.map((view) => (
          <button
            key={view}
            className={active === view ? "active" : ""}
            onClick={() => setActive(view)}
          >
            {view === "Overview" && <Activity size={16} />}
            {view === "Infrastructure" && <Server size={16} />}
            {view === "GitOps" && <GitBranch size={16} />}
            {view === "Data Platform" && <Database size={16} />}
            {view === "Platform Access" && <Compass size={16} />}
            {view === "Incidents" && <ShieldAlert size={16} />}
            {view}
          </button>
        ))}
      </div>
    </aside>
  );
}

function GitOpsPanel({ apps }) {
  return (
    <section className="card gitopsPanel">
      <div className="sectionTitle">
        <span>GitOps Control Plane</span>
      </div>

      <div className="gitopsList">
        {apps.map((app) => (
          <div key={app.name} className={`gitopsRow ${app.severity}`}>
            <div className="gitopsMain">
              <div className={`namespaceDot ${app.severity}`} />
              <div>
                <strong>{app.name}</strong>
                <span>{app.namespace}</span>
              </div>
            </div>

            <div className="gitopsMeta">
              <div className={`gitopsPill ${app.health_status?.toLowerCase()}`}>
                {app.health_status}
              </div>

              <div className={`gitopsPill ${app.sync_status?.toLowerCase()}`}>
                {app.sync_status}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function NamespacePanel({ namespaces }) {
  return (
    <section className="card namespacePanel">
      <div className="sectionTitle">
        <span>Namespace Health Matrix</span>
      </div>

      <div className="namespaceList">
        {namespaces.map((ns) => (
          <div key={ns.name} className={`namespaceRow ${ns.status}`}>
            <div className="namespaceMain">
              <div className={`namespaceDot ${ns.status}`} />
              <strong>{ns.name}</strong>
            </div>

            <div className="namespaceStats">
              <span>{ns.running} running</span>
              <span>{ns.bad} bad</span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function IncidentPanel({ incident }) {
  return (
    <section className={`card incidentCard ${incident?.severity || "healthy"}`}>
      <div className="incidentHeader">
        <div>
          <h2>Operational Incident</h2>
          <span>AI-assisted root cause interpretation</span>
        </div>

        <div className={`incidentBadge ${incident?.severity || "healthy"}`}>
          {(incident?.severity || "healthy").toUpperCase()}
        </div>
      </div>

      <div className="incidentTitle">
        {incident?.title || "No incident detected"}
      </div>

      <div className="incidentSection">
        <label>Root Cause</label>
        <p>{incident?.root_cause}</p>
      </div>

      <div className="incidentSection">
        <label>Impact</label>
        <p>{incident?.impact}</p>
      </div>

      <div className="incidentSection">
        <label>Suggested Action</label>
        <p>{incident?.suggestion}</p>
      </div>
    </section>
  );
}

function App() {
  const [view, setView] = useState("Overview");

  const [data, setData] = useState(null);
  const [topology, setTopology] = useState([]);
  const [events, setEvents] = useState([]);
  const [namespaces, setNamespaces] = useState([]);
  const [argocdApps, setArgocdApps] = useState([]);
  const [incident, setIncident] = useState(null);

  useEffect(() => {
    const load = async () => {
      try {
        const [
          overviewRes,
          topologyRes,
          eventsRes,
          namespacesRes,
          incidentRes,
          argocdRes,
        ] = await Promise.all([
          fetch(`${API_URL}/overview`),
          fetch(`${API_URL}/topology`),
          fetch(`${API_URL}/events`),
          fetch(`${API_URL}/namespaces`),
          fetch(`${API_URL}/incidents`),
          fetch(`${API_URL}/argocd/apps`),
        ]);

        setData(await overviewRes.json());
        setTopology(await topologyRes.json());
        setEvents(await eventsRes.json());
        setNamespaces(await namespacesRes.json());
        setIncident(await incidentRes.json());
        setArgocdApps(await argocdRes.json());
      } catch (err) {
        console.error(err);
      }
    };

    load();

    const id = setInterval(load, 10000);

    return () => clearInterval(id);
  }, []);

  const p = data?.platform;
  const f = data?.fraud;

  return (
    <div className="layout">
      <Sidebar active={view} setActive={setView} />

      <main className="content">
        <header>
          <div>
            <h1>{view}</h1>
            <p>Sovereign Platform Operations Console</p>
          </div>

          <div className="liveWrap">
            <div className="liveDot" />
            LIVE TELEMETRY
          </div>
        </header>

        {view === "Overview" && (
          <>
            <section className="grid top">
              <div className="card hero">
                <div className="heroTop">
                  <Activity size={18} />
                  <span>Platform Health</span>
                </div>

                <div className="score">
                  {p ? `${p.health_score}%` : "--"}
                </div>

                <div className="bar">
                  <div style={{ width: `${p?.health_score || 0}%` }} />
                </div>
              </div>

              <StatCard
                label="Nodes Ready"
                value={p?.nodes_ready ?? "--"}
                sub="OpenShift nodes"
              />

              <StatCard
                label="Bad Pods"
                value={p?.bad_pods ?? "--"}
                sub="Platform failures"
              />

              <StatCard
                label="Argo Synced"
                value={p?.argo_synced ?? "--"}
                sub="GitOps applications"
              />

              <StatCard
                label="Targets Up"
                value={p?.targets_up ?? "--"}
                sub="Prometheus targets"
              />
            </section>

            <IncidentPanel incident={incident} />
          </>
        )}

        {view === "Infrastructure" && (
          <>
            <section className="card topology">
              <div className="sectionTitle">
                <span>Infrastructure Topology</span>
              </div>

              <div className="flow">
                {topology.map((n, i) => (
                  <React.Fragment key={n.title}>
                    <div className={`node ${n.status}`}>
                      <div className="nodeHeader">
                        <div className={`pulse ${n.status}`} />
                        <strong>{n.title}</strong>
                      </div>

                      <span>{n.subtitle}</span>

                      <div className={`statusTag ${n.status}`}>
                        {n.status.toUpperCase()}
                      </div>
                    </div>

                    {i !== topology.length - 1 && (
                      <Zap className="arrow" />
                    )}
                  </React.Fragment>
                ))}
              </div>
            </section>

            <NamespacePanel namespaces={namespaces} />
          </>
        )}

        {view === "GitOps" && (
          <GitOpsPanel apps={argocdApps} />
        )}

        {view === "Data Platform" && (
          <section className="grid bottomWide">
            <StatCard
              label="Transactions"
              value={f?.transactions?.toLocaleString() ?? "--"}
              sub="Protected volume"
            />

            <StatCard
              label="Fraud Cases"
              value={f?.fraud_cases?.toLocaleString() ?? "--"}
              sub="Detected fraud"
              danger
            />

            <StatCard
              label="Fraud Rate"
              value={
                f ? `${(f.fraud_rate * 100).toFixed(2)}%` : "--"
              }
              sub="Risk signal"
            />

            <StatCard
              label="Decisions"
              value={f?.decisions?.toLocaleString() ?? "--"}
              sub="Fraud decisions"
            />

            <div className="card pipelineCard">
              <div className="sectionTitle">
                <span>Pipeline Flow</span>
              </div>

              <div className="pipelineFlow">
                <div>NiFi</div>
                <Workflow size={18} />
                <div>Kafka</div>
                <Workflow size={18} />
                <div>Spark</div>
                <Workflow size={18} />
                <div>Iceberg</div>
                <Workflow size={18} />
                <div>Trino</div>
              </div>
            </div>
          </section>
        )}

        {view === "Platform Access" && (
          <ServiceCatalog />
        )}

        {view === "Incidents" && (
          <>
            <IncidentPanel incident={incident} />

            <section className="card events">
              <div className="sectionTitle">
                <span>Live Operational Events</span>
              </div>

              <div className="eventList">
                {events.map((e, idx) => (
                  <div key={idx} className={`event ${e.level}`}>
                    <div className="eventIcon">
                      {e.level === "critical" ? (
                        <AlertTriangle size={15} />
                      ) : (
                        <CheckCircle2 size={15} />
                      )}
                    </div>

                    <div className="eventBody">
                      <div className="eventText">{e.text}</div>
                      <div className="eventTime">{e.time}</div>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          </>
        )}
      </main>
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
