import React, { useEffect, useMemo, useState } from "react";
import {
  Airplay,
  BarChart3,
  Database,
  ExternalLink,
  GitBranch,
  KeyRound,
  Lock,
  ServerCog,
  Workflow,
  Waves,
  ShieldCheck,
  Boxes,
} from "lucide-react";

import "./serviceCatalog.css";

const API_URL = import.meta.env.VITE_API_URL || "/api";

const iconMap = {
  argocd: GitBranch,
  airflow: Workflow,
  vault: Lock,
  keycloak: KeyRound,
  minio: Database,
  trino: ServerCog,
  superset: BarChart3,
  nifi: Waves,
  grafana: Airplay,
  prometheus: ShieldCheck,
  thanos: Boxes,
};

function ServiceCard({ service }) {
  const Icon = iconMap[service.key] || ServerCog;

  return (
    <a
      className={`serviceCard ${service.status}`}
      href={service.href}
      target="_blank"
      rel="noreferrer"
    >
      <div className="serviceTop">
        <div className="serviceIcon">
          <Icon size={18} />
        </div>

        <div className={`serviceStatus ${service.status}`}>
          <span />
          {service.status.toUpperCase()}
        </div>
      </div>

      <div className="serviceTitle">
        <strong>{service.name}</strong>
        <ExternalLink size={14} />
      </div>

      <p>{service.description}</p>

      <div className="serviceMeta">
        <span>{service.namespace}</span>
        <span>{service.running_pods} running</span>
        <span>{service.bad_pods} bad</span>
      </div>
    </a>
  );
}

export default function ServiceCatalog() {
  const [services, setServices] = useState([]);

  useEffect(() => {
    const load = async () => {
      try {
        const res = await fetch(`${API_URL}/services`);
        setServices(await res.json());
      } catch (err) {
        console.error(err);
      }
    };

    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  const grouped = useMemo(() => {
    return services.reduce((acc, service) => {
      acc[service.category] = acc[service.category] || [];
      acc[service.category].push(service);
      return acc;
    }, {});
  }, [services]);

  return (
    <section className="serviceCatalog">
      <div className="card serviceHero">
        <div>
          <div className="sectionBadge">SERVICE ACCESS LAYER</div>
          <h2>Platform Operations Portal</h2>
          <p>
            Centralized access to platform control planes, security systems,
            data services, and observability tools. Health is evaluated live
            from Prometheus telemetry.
          </p>
        </div>

        <div className="serviceHeroStats">
          <div>
            <span>Total Services</span>
            <strong>{services.length || "--"}</strong>
          </div>
          <div>
            <span>Healthy</span>
            <strong>{services.filter((s) => s.status === "healthy").length}</strong>
          </div>
          <div>
            <span>At Risk</span>
            <strong>
              {services.filter((s) => s.status !== "healthy").length}
            </strong>
          </div>
        </div>
      </div>

      {Object.entries(grouped).map(([category, items]) => (
        <div key={category} className="serviceGroup">
          <div className="sectionTitle">
            <span>{category}</span>
          </div>

          <div className="serviceGrid">
            {items.map((service) => (
              <ServiceCard key={service.key} service={service} />
            ))}
          </div>
        </div>
      ))}
    </section>
  );
}
