-- ============================================================
-- Fraud Detection Platform — Iceberg DDL
-- Catalog: iceberg (Hive Metastore)
-- Storage: s3a://warehouse/ (MinIO)
-- ============================================================

-- SCHEMAS
CREATE SCHEMA IF NOT EXISTS iceberg.warehouse_bronze WITH (location = 's3a://warehouse/bronze');
CREATE SCHEMA IF NOT EXISTS iceberg.warehouse_silver WITH (location = 's3a://warehouse/silver');
CREATE SCHEMA IF NOT EXISTS iceberg.warehouse_gold   WITH (location = 's3a://warehouse/gold');

-- ============================================================
-- BRONZE — Raw landing, append-only, no transforms
-- ============================================================
CREATE TABLE IF NOT EXISTS iceberg.warehouse_bronze.transactions (
  ingestion_id      VARCHAR,
  kafka_offset      BIGINT,
  kafka_partition   INTEGER,
  ingested_at       TIMESTAMP(6) WITH TIME ZONE,
  ingestion_date    DATE,
  TransactionID     INTEGER,
  isFraud           INTEGER,
  TransactionDT     INTEGER,
  TransactionAmt    DOUBLE,
  ProductCD         VARCHAR,
  card1 DOUBLE, card2 DOUBLE, card3 DOUBLE, card4 VARCHAR, card5 DOUBLE, card6 VARCHAR,
  addr1 DOUBLE, addr2 DOUBLE, dist1 DOUBLE, dist2 DOUBLE,
  P_emaildomain VARCHAR, R_emaildomain VARCHAR,
  C1 DOUBLE, C2 DOUBLE, C3 DOUBLE, C4 DOUBLE, C5 DOUBLE, C6 DOUBLE, C7 DOUBLE,
  C8 DOUBLE, C9 DOUBLE, C10 DOUBLE, C11 DOUBLE, C12 DOUBLE, C13 DOUBLE, C14 DOUBLE,
  D1 DOUBLE, D2 DOUBLE, D3 DOUBLE, D4 DOUBLE, D5 DOUBLE, D6 DOUBLE, D7 DOUBLE,
  D8 DOUBLE, D9 DOUBLE, D10 DOUBLE, D11 DOUBLE, D12 DOUBLE, D13 DOUBLE, D14 DOUBLE, D15 DOUBLE,
  M1 VARCHAR, M2 VARCHAR, M3 VARCHAR, M4 VARCHAR, M5 VARCHAR,
  M6 VARCHAR, M7 VARCHAR, M8 VARCHAR, M9 VARCHAR,
  V1 DOUBLE, V2 DOUBLE, V3 DOUBLE, V4 DOUBLE, V5 DOUBLE, V6 DOUBLE, V7 DOUBLE,
  V8 DOUBLE, V9 DOUBLE, V10 DOUBLE, V11 DOUBLE, V12 DOUBLE, V13 DOUBLE, V14 DOUBLE,
  V15 DOUBLE, V16 DOUBLE, V17 DOUBLE, V18 DOUBLE, V19 DOUBLE, V20 DOUBLE, V21 DOUBLE,
  V22 DOUBLE, V23 DOUBLE, V24 DOUBLE, V25 DOUBLE, V26 DOUBLE, V27 DOUBLE, V28 DOUBLE,
  V29 DOUBLE, V30 DOUBLE, V31 DOUBLE, V32 DOUBLE, V33 DOUBLE, V34 DOUBLE, V35 DOUBLE,
  V36 DOUBLE, V37 DOUBLE, V38 DOUBLE, V39 DOUBLE, V40 DOUBLE, V41 DOUBLE, V42 DOUBLE,
  V43 DOUBLE, V44 DOUBLE, V45 DOUBLE, V46 DOUBLE, V47 DOUBLE, V48 DOUBLE, V49 DOUBLE,
  V50 DOUBLE, V51 DOUBLE, V52 DOUBLE, V53 DOUBLE, V54 DOUBLE, V55 DOUBLE, V56 DOUBLE,
  V57 DOUBLE, V58 DOUBLE, V59 DOUBLE, V60 DOUBLE, V61 DOUBLE, V62 DOUBLE, V63 DOUBLE,
  V64 DOUBLE, V65 DOUBLE, V66 DOUBLE, V67 DOUBLE, V68 DOUBLE, V69 DOUBLE, V70 DOUBLE,
  V71 DOUBLE, V72 DOUBLE, V73 DOUBLE, V74 DOUBLE, V75 DOUBLE, V76 DOUBLE, V77 DOUBLE,
  V78 DOUBLE, V79 DOUBLE, V80 DOUBLE, V81 DOUBLE, V82 DOUBLE, V83 DOUBLE, V84 DOUBLE,
  V85 DOUBLE, V86 DOUBLE, V87 DOUBLE, V88 DOUBLE, V89 DOUBLE, V90 DOUBLE, V91 DOUBLE,
  V92 DOUBLE, V93 DOUBLE, V94 DOUBLE, V95 DOUBLE, V96 DOUBLE, V97 DOUBLE, V98 DOUBLE,
  V99 DOUBLE, V100 DOUBLE, V101 DOUBLE, V102 DOUBLE, V103 DOUBLE, V104 DOUBLE,
  V105 DOUBLE, V106 DOUBLE, V107 DOUBLE, V108 DOUBLE, V109 DOUBLE, V110 DOUBLE,
  V111 DOUBLE, V112 DOUBLE, V113 DOUBLE, V114 DOUBLE, V115 DOUBLE, V116 DOUBLE,
  V117 DOUBLE, V118 DOUBLE, V119 DOUBLE, V120 DOUBLE, V121 DOUBLE, V122 DOUBLE,
  V123 DOUBLE, V124 DOUBLE, V125 DOUBLE, V126 DOUBLE, V127 DOUBLE, V128 DOUBLE,
  V129 DOUBLE, V130 DOUBLE, V131 DOUBLE, V132 DOUBLE, V133 DOUBLE, V134 DOUBLE,
  V135 DOUBLE, V136 DOUBLE, V137 DOUBLE, V138 DOUBLE, V139 DOUBLE, V140 DOUBLE,
  V141 DOUBLE, V142 DOUBLE, V143 DOUBLE, V144 DOUBLE, V145 DOUBLE, V146 DOUBLE,
  V147 DOUBLE, V148 DOUBLE, V149 DOUBLE, V150 DOUBLE, V151 DOUBLE, V152 DOUBLE,
  V153 DOUBLE, V154 DOUBLE, V155 DOUBLE, V156 DOUBLE, V157 DOUBLE, V158 DOUBLE,
  V159 DOUBLE, V160 DOUBLE, V161 DOUBLE, V162 DOUBLE, V163 DOUBLE, V164 DOUBLE,
  V165 DOUBLE, V166 DOUBLE, V167 DOUBLE, V168 DOUBLE, V169 DOUBLE, V170 DOUBLE,
  V171 DOUBLE, V172 DOUBLE, V173 DOUBLE, V174 DOUBLE, V175 DOUBLE, V176 DOUBLE,
  V177 DOUBLE, V178 DOUBLE, V179 DOUBLE, V180 DOUBLE, V181 DOUBLE, V182 DOUBLE,
  V183 DOUBLE, V184 DOUBLE, V185 DOUBLE, V186 DOUBLE, V187 DOUBLE, V188 DOUBLE,
  V189 DOUBLE, V190 DOUBLE, V191 DOUBLE, V192 DOUBLE, V193 DOUBLE, V194 DOUBLE,
  V195 DOUBLE, V196 DOUBLE, V197 DOUBLE, V198 DOUBLE, V199 DOUBLE, V200 DOUBLE,
  V201 DOUBLE, V202 DOUBLE, V203 DOUBLE, V204 DOUBLE, V205 DOUBLE, V206 DOUBLE,
  V207 DOUBLE, V208 DOUBLE, V209 DOUBLE, V210 DOUBLE, V211 DOUBLE, V212 DOUBLE,
  V213 DOUBLE, V214 DOUBLE, V215 DOUBLE, V216 DOUBLE, V217 DOUBLE, V218 DOUBLE,
  V219 DOUBLE, V220 DOUBLE, V221 DOUBLE, V222 DOUBLE, V223 DOUBLE, V224 DOUBLE,
  V225 DOUBLE, V226 DOUBLE, V227 DOUBLE, V228 DOUBLE, V229 DOUBLE, V230 DOUBLE,
  V231 DOUBLE, V232 DOUBLE, V233 DOUBLE, V234 DOUBLE, V235 DOUBLE, V236 DOUBLE,
  V237 DOUBLE, V238 DOUBLE, V239 DOUBLE, V240 DOUBLE, V241 DOUBLE, V242 DOUBLE,
  V243 DOUBLE, V244 DOUBLE, V245 DOUBLE, V246 DOUBLE, V247 DOUBLE, V248 DOUBLE,
  V249 DOUBLE, V250 DOUBLE, V251 DOUBLE, V252 DOUBLE, V253 DOUBLE, V254 DOUBLE,
  V255 DOUBLE, V256 DOUBLE, V257 DOUBLE, V258 DOUBLE, V259 DOUBLE, V260 DOUBLE,
  V261 DOUBLE, V262 DOUBLE, V263 DOUBLE, V264 DOUBLE, V265 DOUBLE, V266 DOUBLE,
  V267 DOUBLE, V268 DOUBLE, V269 DOUBLE, V270 DOUBLE, V271 DOUBLE, V272 DOUBLE,
  V273 DOUBLE, V274 DOUBLE, V275 DOUBLE, V276 DOUBLE, V277 DOUBLE, V278 DOUBLE,
  V279 DOUBLE, V280 DOUBLE, V281 DOUBLE, V282 DOUBLE, V283 DOUBLE, V284 DOUBLE,
  V285 DOUBLE, V286 DOUBLE, V287 DOUBLE, V288 DOUBLE, V289 DOUBLE, V290 DOUBLE,
  V291 DOUBLE, V292 DOUBLE, V293 DOUBLE, V294 DOUBLE, V295 DOUBLE, V296 DOUBLE,
  V297 DOUBLE, V298 DOUBLE, V299 DOUBLE, V300 DOUBLE, V301 DOUBLE, V302 DOUBLE,
  V303 DOUBLE, V304 DOUBLE, V305 DOUBLE, V306 DOUBLE, V307 DOUBLE, V308 DOUBLE,
  V309 DOUBLE, V310 DOUBLE, V311 DOUBLE, V312 DOUBLE, V313 DOUBLE, V314 DOUBLE,
  V315 DOUBLE, V316 DOUBLE, V317 DOUBLE, V318 DOUBLE, V319 DOUBLE, V320 DOUBLE,
  V321 DOUBLE, V322 DOUBLE, V323 DOUBLE, V324 DOUBLE, V325 DOUBLE, V326 DOUBLE,
  V327 DOUBLE, V328 DOUBLE, V329 DOUBLE, V330 DOUBLE, V331 DOUBLE, V332 DOUBLE,
  V333 DOUBLE, V334 DOUBLE, V335 DOUBLE, V336 DOUBLE, V337 DOUBLE, V338 DOUBLE, V339 DOUBLE,
  id_01 DOUBLE, id_02 DOUBLE, id_03 DOUBLE, id_04 DOUBLE, id_05 DOUBLE,
  id_06 DOUBLE, id_07 DOUBLE, id_08 DOUBLE, id_09 DOUBLE, id_10 DOUBLE,
  id_11 DOUBLE, id_12 VARCHAR, id_13 DOUBLE, id_14 DOUBLE, id_15 VARCHAR,
  id_16 VARCHAR, id_17 DOUBLE, id_18 DOUBLE, id_19 DOUBLE, id_20 DOUBLE,
  id_21 DOUBLE, id_22 DOUBLE, id_23 DOUBLE, id_24 DOUBLE, id_25 DOUBLE,
  id_26 DOUBLE, id_27 DOUBLE, id_28 VARCHAR, id_29 VARCHAR, id_30 VARCHAR,
  id_31 VARCHAR, id_32 DOUBLE, id_33 VARCHAR, id_34 VARCHAR,
  id_35 VARCHAR, id_36 VARCHAR, id_37 VARCHAR, id_38 VARCHAR,
  DeviceType VARCHAR, DeviceInfo VARCHAR
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(ingestion_date)'],
  location = 's3a://warehouse/bronze/transactions'
);

-- ============================================================
-- SILVER — Cleaned, typed, engineered
-- ============================================================
CREATE TABLE IF NOT EXISTS iceberg.warehouse_silver.transactions (
  ingestion_id VARCHAR, ingested_at TIMESTAMP(6) WITH TIME ZONE, transaction_date DATE,
  TransactionID INTEGER, isFraud INTEGER,
  TransactionDT INTEGER,
  event_timestamp TIMESTAMP(6) WITH TIME ZONE,
  transaction_hour INTEGER, transaction_dow INTEGER,
  TransactionAmt DOUBLE, cent_amt DOUBLE, ProductCD VARCHAR,
  card1 DOUBLE, card2 DOUBLE, card3 DOUBLE, card4 VARCHAR, card5 DOUBLE, card6 VARCHAR,
  uid VARCHAR,
  addr1 DOUBLE, addr2 DOUBLE, dist1 DOUBLE, dist2 DOUBLE,
  P_emaildomain VARCHAR, R_emaildomain VARCHAR,
  C1 DOUBLE, C2 DOUBLE, C3 DOUBLE, C4 DOUBLE, C5 DOUBLE, C6 DOUBLE, C7 DOUBLE,
  C8 DOUBLE, C9 DOUBLE, C10 DOUBLE, C11 DOUBLE, C12 DOUBLE, C13 DOUBLE, C14 DOUBLE,
  D1 DOUBLE, D2 DOUBLE, D3 DOUBLE, D4 DOUBLE, D5 DOUBLE, D6 DOUBLE, D7 DOUBLE,
  D8 DOUBLE, D9 DOUBLE, D10 DOUBLE, D11 DOUBLE, D12 DOUBLE, D13 DOUBLE, D14 DOUBLE, D15 DOUBLE,
  M1 BOOLEAN, M2 BOOLEAN, M3 BOOLEAN, M4 VARCHAR,
  M5 BOOLEAN, M6 BOOLEAN, M7 BOOLEAN, M8 BOOLEAN, M9 BOOLEAN,
  V258 DOUBLE, V257 DOUBLE, V201 DOUBLE, V200 DOUBLE,
  V130 DOUBLE, V131 DOUBLE, V132 DOUBLE, V133 DOUBLE, V134 DOUBLE, V135 DOUBLE,
  V136 DOUBLE, V137 DOUBLE,
  V95 DOUBLE, V96 DOUBLE, V97 DOUBLE, V98 DOUBLE, V99 DOUBLE,
  V100 DOUBLE, V101 DOUBLE, V102 DOUBLE,
  has_identity BOOLEAN,
  id_01 DOUBLE, id_02 DOUBLE, id_03 DOUBLE, id_04 DOUBLE, id_05 DOUBLE,
  id_06 DOUBLE, id_07 DOUBLE, id_08 DOUBLE, id_09 DOUBLE, id_10 DOUBLE,
  id_11 DOUBLE, id_12 VARCHAR, id_13 DOUBLE, id_14 DOUBLE, id_15 VARCHAR,
  id_16 VARCHAR, id_17 DOUBLE, id_18 DOUBLE, id_19 DOUBLE, id_20 DOUBLE,
  id_21 DOUBLE, id_22 DOUBLE, id_23 DOUBLE, id_24 DOUBLE, id_25 DOUBLE,
  id_26 DOUBLE, id_27 DOUBLE, id_28 VARCHAR, id_29 VARCHAR, id_30 VARCHAR,
  id_31 VARCHAR, id_32 DOUBLE, id_33 VARCHAR, id_34 VARCHAR,
  id_35 VARCHAR, id_36 VARCHAR, id_37 VARCHAR, id_38 VARCHAR,
  DeviceType VARCHAR, DeviceInfo VARCHAR
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(transaction_date)', 'ProductCD'],
  location = 's3a://warehouse/silver/transactions'
);

CREATE TABLE IF NOT EXISTS iceberg.warehouse_silver.decisions (
  ingestion_id VARCHAR, decided_at TIMESTAMP(6) WITH TIME ZONE, transaction_date DATE,
  TransactionID INTEGER, uid VARCHAR, TransactionAmt DOUBLE, ProductCD VARCHAR,
  card4 VARCHAR, card6 VARCHAR, P_emaildomain VARCHAR, transaction_hour INTEGER,
  fraud_score DOUBLE, decision VARCHAR, decision_reason VARCHAR,
  model_version VARCHAR, rules_triggered VARCHAR, isFraud INTEGER
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(transaction_date)', 'decision'],
  location = 's3a://warehouse/silver/decisions'
);

-- ============================================================
-- GOLD — Aggregated, Superset-ready
-- ============================================================
CREATE TABLE IF NOT EXISTS iceberg.warehouse_gold.daily_fraud_summary (
  summary_date DATE, total_transactions BIGINT, total_amount DOUBLE,
  fraud_count BIGINT, fraud_amount DOUBLE, fraud_rate DOUBLE,
  avg_fraud_amount DOUBLE, max_fraud_amount DOUBLE,
  blocked_count BIGINT, blocked_amount DOUBLE,
  updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(summary_date)'],
      location = 's3a://warehouse/gold/daily_fraud_summary');

CREATE TABLE IF NOT EXISTS iceberg.warehouse_gold.product_fraud_stats (
  summary_date DATE, ProductCD VARCHAR, total_transactions BIGINT,
  total_amount DOUBLE, fraud_count BIGINT, fraud_rate DOUBLE,
  avg_transaction_amt DOUBLE, avg_fraud_amt DOUBLE,
  updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(summary_date)'],
      location = 's3a://warehouse/gold/product_fraud_stats');

CREATE TABLE IF NOT EXISTS iceberg.warehouse_gold.hourly_fraud_pattern (
  summary_date DATE, transaction_hour INTEGER, transaction_dow INTEGER,
  total_transactions BIGINT, fraud_count BIGINT, fraud_rate DOUBLE,
  avg_amount DOUBLE, risk_level VARCHAR,
  updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(summary_date)'],
      location = 's3a://warehouse/gold/hourly_fraud_pattern');

CREATE TABLE IF NOT EXISTS iceberg.warehouse_gold.uid_risk_profile (
  uid VARCHAR, card1 DOUBLE, addr1 DOUBLE,
  total_transactions BIGINT, total_amount DOUBLE, fraud_count BIGINT,
  fraud_rate DOUBLE, avg_amount DOUBLE, max_amount DOUBLE,
  distinct_merchants BIGINT, distinct_emails BIGINT,
  last_seen TIMESTAMP(6) WITH TIME ZONE, risk_tier VARCHAR,
  updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['risk_tier'],
      location = 's3a://warehouse/gold/uid_risk_profile');

CREATE TABLE IF NOT EXISTS iceberg.warehouse_gold.rule_performance (
  summary_date DATE, rule_name VARCHAR, rule_type VARCHAR,
  triggers_count BIGINT, true_positive BIGINT, false_positive BIGINT,
  true_negative BIGINT, false_negative BIGINT,
  precision_score DOUBLE, recall_score DOUBLE, f1_score DOUBLE,
  blocked_amount DOUBLE, updated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(summary_date)'],
      location = 's3a://warehouse/gold/rule_performance');

-- ============================================================
-- GOLD VIEWS (Trino semantic layer — Superset queries these)
-- ============================================================
CREATE OR REPLACE VIEW iceberg.warehouse_gold.fraud_rate_by_product AS
SELECT ProductCD, COUNT(*) AS total_transactions,
  SUM(CAST(isFraud AS BIGINT)) AS fraud_count,
  ROUND(AVG(CAST(isFraud AS DOUBLE)) * 100, 3) AS fraud_rate_pct,
  ROUND(AVG(TransactionAmt), 2) AS avg_transaction_amt,
  ROUND(SUM(CASE WHEN isFraud = 1 THEN TransactionAmt ELSE 0 END), 2) AS total_fraud_amt
FROM iceberg.warehouse_silver.transactions GROUP BY ProductCD;

CREATE OR REPLACE VIEW iceberg.warehouse_gold.hourly_fraud_trend AS
SELECT transaction_hour, COUNT(*) AS total_transactions,
  SUM(CAST(isFraud AS BIGINT)) AS fraud_count,
  ROUND(AVG(CAST(isFraud AS DOUBLE)) * 100, 3) AS fraud_rate_pct,
  ROUND(AVG(TransactionAmt), 2) AS avg_transaction_amt
FROM iceberg.warehouse_silver.transactions GROUP BY transaction_hour ORDER BY transaction_hour;

CREATE OR REPLACE VIEW iceberg.warehouse_gold.top_risky_uids AS
SELECT uid, COUNT(*) AS total_transactions,
  SUM(CAST(isFraud AS BIGINT)) AS fraud_count,
  ROUND(AVG(CAST(isFraud AS DOUBLE)) * 100, 3) AS fraud_rate_pct,
  ROUND(SUM(TransactionAmt), 2) AS total_amount,
  ROUND(AVG(TransactionAmt), 2) AS avg_amount,
  MAX(transaction_date) AS last_seen
FROM iceberg.warehouse_silver.transactions
WHERE uid IS NOT NULL GROUP BY uid HAVING COUNT(*) >= 3 ORDER BY fraud_rate_pct DESC;

CREATE OR REPLACE VIEW iceberg.warehouse_gold.rule_effectiveness_ranking AS
SELECT rule_name, rule_type, SUM(triggers_count) AS total_triggers,
  SUM(true_positive) AS total_tp, SUM(false_positive) AS total_fp,
  ROUND(AVG(precision_score) * 100, 2) AS avg_precision_pct,
  ROUND(AVG(recall_score) * 100, 2) AS avg_recall_pct,
  ROUND(AVG(f1_score) * 100, 2) AS avg_f1_pct,
  ROUND(SUM(blocked_amount), 2) AS total_blocked_amount
FROM iceberg.warehouse_gold.rule_performance GROUP BY rule_name, rule_type ORDER BY avg_f1_pct DESC;

CREATE OR REPLACE VIEW iceberg.warehouse_gold.daily_blocked_amount AS
SELECT summary_date, total_transactions, fraud_count,
  ROUND(fraud_rate * 100, 3) AS fraud_rate_pct, blocked_count,
  ROUND(blocked_amount, 2) AS blocked_amount,
  ROUND(fraud_amount, 2) AS fraud_amount,
  ROUND(blocked_amount / NULLIF(fraud_amount, 0) * 100, 2) AS capture_rate_pct
FROM iceberg.warehouse_gold.daily_fraud_summary ORDER BY summary_date DESC;
