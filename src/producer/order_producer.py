import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


RESTAURANTS = [
    {"restaurant_id": "REST-101", "name": "Spice Hub"},
    {"restaurant_id": "REST-102", "name": "Burger Junction"},
    {"restaurant_id": "REST-103", "name": "Pizza Corner"},
    {"restaurant_id": "REST-104", "name": "South Feast"},
    {"restaurant_id": "REST-105", "name": "Noodle House"},
]

MENU = {
    "REST-101": [("Paneer Biryani", 280), ("Veg Thali", 220)],
    "REST-102": [("Veg Burger", 150), ("French Fries", 100)],
    "REST-103": [("Margherita Pizza", 250), ("Farmhouse Pizza", 350)],
    "REST-104": [("Masala Dosa", 120), ("Idli Sambar", 90)],
    "REST-105": [("Veg Hakka Noodles", 180), ("Manchurian", 200)],
}

ORDER_STATUSES = ["PLACED", "CONFIRMED", "PREPARING"]


def generate_order() -> dict:
    """Generate one realistic food-delivery order event."""
    restaurant = random.choice(RESTAURANTS)
    restaurant_id = restaurant["restaurant_id"]
    item, price = random.choice(MENU[restaurant_id])
    quantity = random.randint(1, 4)

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": f"CUST-{random.randint(1001, 1100)}",
        "restaurant_id": restaurant_id,
        "restaurant_name": restaurant["name"],
        "item": item,
        "quantity": quantity,
        "unit_price": price,
        "total_amount": round(quantity * price, 2),
        "status": random.choice(ORDER_STATUSES),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )

    print(f"StreamPulse producer started. Topic: {KAFKA_TOPIC}")
    print("Generating orders every 2 seconds. Press Ctrl+C to stop.")

    try:
        while True:
            order = generate_order()
            future = producer.send(KAFKA_TOPIC, value=order)
            metadata = future.get(timeout=10)

            print(
                f"Sent order {order['order_id']} | "
                f"{order['item']} | "
                f"Amount: Rs. {order['total_amount']:.2f} | "
                f"Partition: {metadata.partition}"
            )

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping order producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main()
