# Observability

## Overview

The platform uses Prometheus, Thanos, Grafana, PrometheusRule resources, ServiceMonitors, and a custom fraud metrics exporter.

Observability covers both infrastructure signals and business-level fraud metrics.

---

## Components

| Component | Purpose |
|---|---|
| Prometheus | Metrics collection |
| Thanos | Query layer |
| Grafana | Dashboards |
| ServiceMonitor | Scrape target definition |
| PrometheusRule | Alert definitions |
| Fraud metrics exporter | Converts Trino gold data into Prometheus metrics |
| Command Center | Summarized operational interface |

---

## Fraud Metrics Exporter

Validated exporter metrics include:

```text
fraud_total_transactions_all 994376
fraud_total_fraud_count_all 34391
fraud_avg_fraud_rate_all 0.03604427910797532
fraud_total_blocked_all 7
fraud_silver_decisions_count 994376
fraud_silver_transactions_count 994376
```

Product-level metrics were also exposed for products C, S, H, R, and W.

---

## Product Fraud Rates

Validated examples:

| Product | Fraud Rate |
|---|---:|
| C | 11.30% |
| S | 5.87% |
| H | 4.50% |
| R | 3.60% |
| W | 2.05% |

---

## Alerting

A PrometheusRule named `fraud-platform-alerts` exists in the `kafka` namespace.

The rule file includes alerts such as:

```text
FraudRateSpikeDetected
FraudPipelineDLQNonZero
KafkaBrokerDown
SparkExecutorOOM
HiveMetastoreDown
TrinoDown
```

The README states 12 Prometheus alerting rules because that is the intended fraud-platform alert set in the repo.

---

## Grafana

Grafana is running in the `monitoring` namespace.

Grafana is used for dashboards and metric visualization, while the Command Center is used as the higher-level operations console.

---

## Important Note

The fraud exporter container did not include `curl`, so exporter validation was done through a temporary curl pod instead of execing directly into the exporter container. That is normal and should not be treated as an exporter failure.
