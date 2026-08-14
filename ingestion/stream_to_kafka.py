import json
import os
import time

import websocket
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "binance_trades")
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@aggTrade")


def wait_for_kafka(bootstrap_servers, max_retries=30, delay=5):
    """Đợi Kafka sẵn sàng trước khi bắt đầu stream"""
    from confluent_kafka.admin import AdminClient
    for attempt in range(1, max_retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin.list_topics(timeout=5)
            print(f"✅ Kafka sẵn sàng! Brokers: {len(metadata.brokers)}")
            return True
        except Exception as e:
            print(f"⏳ [{attempt}/{max_retries}] Đợi Kafka... ({e})")
            time.sleep(delay)
    raise ConnectionError(f"Không thể kết nối Kafka sau {max_retries} lần thử")


def delivery_report(err, msg):
    """Callback để log lỗi khi gửi message thất bại"""
    if err is not None:
        print(f"❌ Delivery failed: {err}")


# Đợi Kafka sẵn sàng
wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.ms": 500,
})


def on_message(ws, message):
    data = json.loads(message)
    normalized_data = {
        "a": data.get("a"),
        "s": data.get("s"),
        "p": data.get("p"),
        "q": data.get("q"),
        "T": data.get("T"),
        "m": data.get("m")
    }

    producer.produce(
        TOPIC_NAME,
        value=json.dumps(normalized_data),
        callback=delivery_report
    )
    producer.poll(0)  # Trigger delivery callbacks without blocking

    print(
        f"Real-time: Price {normalized_data['p']} | "
        f"Qty {normalized_data['q']} | "
        f"BuyerMaker {normalized_data['m']} | "
        f"Time {normalized_data['T']}"
    )


def on_error(ws, error):
    print(f"⚠️ Lỗi kết nối WebSocket: {error}")


def on_close(ws, close_status_code, close_msg):
    producer.flush(timeout=10)
    print("🔌 Đã ngắt kết nối với Binance")


def on_open(ws):
    print("🟢 Đã kết nối thành công với Binance WebSocket")


ws = websocket.WebSocketApp(
    BINANCE_WS_URL,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

print(
    f"🚀 Đang stream Binance -> Kafka. topic={TOPIC_NAME}, "
    f"bootstrap={KAFKA_BOOTSTRAP_SERVERS}"
)

# Tự động reconnect khi mất kết nối
while True:
    try:
        ws.run_forever(ping_interval=30, ping_timeout=10)
    except Exception as e:
        print(f"⚠️ WebSocket bị ngắt: {e}, reconnect sau 5s...")
    time.sleep(5)
    print("🔄 Đang reconnect...")
