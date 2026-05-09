"""
Fraud Platform Metrics Exporter
Queries Trino gold tables and exposes as Prometheus metrics.
Scrape interval: 60s
"""
import time
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler

TRINO_URL = "http://trino.trino.svc:8080"
PORT = 9090
SCRAPE_INTERVAL = 60

metrics_cache = {}
last_scrape = 0

def run_trino_query(sql):
    """Execute Trino query via REST API and return rows."""
    try:
        resp = requests.post(
            f"{TRINO_URL}/v1/statement",
            headers={"X-Trino-User": "prometheus", "Content-Type": "text/plain"},
            data=sql,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        
        # Follow nextUri until done
        rows = []
        columns = []
        while True:
            if "columns" in data:
                columns = [c["name"] for c in data["columns"]]
            if "data" in data:
                rows.extend(data["data"])
            if "nextUri" not in data:
                break
            time.sleep(0.1)
            resp = requests.get(data["nextUri"], 
                headers={"X-Trino-User": "prometheus"},
                timeout=30)
            resp.raise_for_status()
            data = resp.json()
        
        return columns, rows
    except Exception as e:
        print(f"Trino query error: {e}")
        return [], []

def collect_metrics():
    """Query all gold tables and build metrics dict."""
    global metrics_cache
    m = {}

    # --- daily_fraud_summary: latest day ---
    cols, rows = run_trino_query("""
        SELECT 
          total_transactions,
          total_amount,
          fraud_count,
          fraud_rate,
          blocked_count,
          blocked_amount
        FROM iceberg.warehouse_gold.daily_fraud_summary
        ORDER BY summary_date DESC
        LIMIT 1
    """)
    if rows:
        r = rows[0]
        m["fraud_daily_total_transactions"] = r[0] or 0
        m["fraud_daily_total_amount"] = r[1] or 0
        m["fraud_daily_fraud_count"] = r[2] or 0
        m["fraud_daily_fraud_rate"] = r[3] or 0
        m["fraud_daily_blocked_count"] = r[4] or 0
        m["fraud_daily_blocked_amount"] = r[5] or 0

    # --- daily_fraud_summary: totals across all days ---
    cols, rows = run_trino_query("""
        SELECT 
          SUM(total_transactions),
          SUM(fraud_count),
          AVG(fraud_rate),
          SUM(blocked_count),
          COUNT(DISTINCT summary_date)
        FROM iceberg.warehouse_gold.daily_fraud_summary
    """)
    if rows:
        r = rows[0]
        m["fraud_total_transactions_all"] = r[0] or 0
        m["fraud_total_fraud_count_all"] = r[1] or 0
        m["fraud_avg_fraud_rate_all"] = r[2] or 0
        m["fraud_total_blocked_all"] = r[3] or 0
        m["fraud_days_tracked"] = r[4] or 0

    # --- product_fraud_stats ---
    cols, rows = run_trino_query("""
        SELECT productcd, total_transactions, fraud_count, fraud_rate
        FROM iceberg.warehouse_gold.product_fraud_stats
        ORDER BY fraud_rate DESC
    """)
    for row in rows:
        product = row[0]
        m[f"fraud_product_transactions{{product=\"{product}\"}}"] = row[1] or 0
        m[f"fraud_product_fraud_count{{product=\"{product}\"}}"] = row[2] or 0
        m[f"fraud_product_fraud_rate{{product=\"{product}\"}}"] = row[3] or 0

    # --- hourly_fraud_pattern ---
    cols, rows = run_trino_query("""
        SELECT transaction_hour, total_transactions, fraud_count, fraud_rate, risk_level
        FROM iceberg.warehouse_gold.hourly_fraud_pattern
        ORDER BY transaction_hour
    """)
    for row in rows:
        hour = row[0]
        risk = row[4] or "LOW"
        m[f"fraud_hourly_transactions{{hour=\"{hour}\",risk=\"{risk}\"}}"] = row[1] or 0
        m[f"fraud_hourly_fraud_rate{{hour=\"{hour}\",risk=\"{risk}\"}}"] = row[3] or 0

    # --- uid_risk_profile: tier counts ---
    cols, rows = run_trino_query("""
        SELECT risk_tier, COUNT(*) as cnt, AVG(fraud_rate) as avg_rate
        FROM iceberg.warehouse_gold.uid_risk_profile
        GROUP BY risk_tier
    """)
    for row in rows:
        tier = row[0]
        m[f"fraud_uid_risk_count{{tier=\"{tier}\"}}"] = row[1] or 0
        m[f"fraud_uid_avg_fraud_rate{{tier=\"{tier}\"}}"] = row[2] or 0

    # --- rule_performance ---
    cols, rows = run_trino_query("""
        SELECT rule_name, triggers_count, true_positive, false_positive, precision_score
        FROM iceberg.warehouse_gold.rule_performance
    """)
    for row in rows:
        rule = row[0]
        m[f"fraud_rule_triggers{{rule=\"{rule}\"}}"] = row[1] or 0
        m[f"fraud_rule_true_positive{{rule=\"{rule}\"}}"] = row[2] or 0
        m[f"fraud_rule_false_positive{{rule=\"{rule}\"}}"] = row[3] or 0
        m[f"fraud_rule_precision{{rule=\"{rule}\"}}"] = row[4] or 0

    # --- silver table sizes ---
    cols, rows = run_trino_query("""
        SELECT COUNT(*) FROM iceberg.warehouse_silver.decisions
    """)
    if rows:
        m["fraud_silver_decisions_count"] = rows[0][0] or 0

    cols, rows = run_trino_query("""
        SELECT COUNT(*) FROM iceberg.warehouse_silver.transactions
    """)
    if rows:
        m["fraud_silver_transactions_count"] = rows[0][0] or 0

    metrics_cache = m
    print(f"Collected {len(m)} metrics")

def format_prometheus(metrics):
    """Format metrics dict as Prometheus text format."""
    lines = []
    for key, value in metrics.items():
        lines.append(f"{key} {value}")
    return "\n".join(lines) + "\n"

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global last_scrape
        if self.path == "/metrics":
            now = time.time()
            if now - last_scrape > SCRAPE_INTERVAL:
                collect_metrics()
                last_scrape = now
            output = format_prometheus(metrics_cache)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(output.encode())
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress access logs

if __name__ == "__main__":
    print(f"Starting fraud metrics exporter on port {PORT}")
    print("Collecting initial metrics...")
    collect_metrics()
    last_scrape = time.time()
    server = HTTPServer(("0.0.0.0", PORT), MetricsHandler)
    print(f"Serving metrics at http://0.0.0.0:{PORT}/metrics")
    server.serve_forever()
