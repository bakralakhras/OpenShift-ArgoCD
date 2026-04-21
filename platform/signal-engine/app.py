from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict, deque
import statistics

KAFKA_BOOTSTRAP = "lakehouse-kafka-kafka-bootstrap:9092"

consumer = KafkaConsumer(
    "crypto-prices",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

windows = defaultdict(lambda: deque(maxlen=50))

def compute(symbol, data):
    prices = [x["price"] for x in windows[symbol]]

    if len(prices) < 10:
        return None

    ma = sum(prices) / len(prices)
    deviation = ((data["price"] - ma) / ma) * 100
    vol = statistics.pstdev(prices) if len(prices) > 1 else 0

    severity = "NORMAL"
    if abs(deviation) > 0.1:
        severity = "WATCH"
    if abs(deviation) > 0.3:
        severity = "ALERT"
    if abs(deviation) > 0.5:
        severity = "CRITICAL"

    return {
        "symbol": symbol,
        "price": data["price"],
        "ingested_at": data["ingested_at"],
        "ma": ma,
        "deviation_pct": deviation,
        "volatility": vol,
        "severity": severity
    }

for msg in consumer:
    data = msg.value
    symbol = data["symbol"]

    windows[symbol].append(data)

    signal = compute(symbol, data)
    if not signal:
        continue

    producer.send("crypto-signals", signal)

    if signal["severity"] in ["ALERT", "CRITICAL"]:
        producer.send("crypto-alerts", {
            "symbol": symbol,
            "severity": signal["severity"],
            "deviation_pct": signal["deviation_pct"],
            "price": signal["price"],
            "ingested_at": signal["ingested_at"]
        })
