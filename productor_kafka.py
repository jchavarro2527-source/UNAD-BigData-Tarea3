import json, time, random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(">>> Iniciando transmisión continua de transacciones e-commerce a Kafka... (Presiona Ctrl+C para detener)")
i = 1
try:
    while True:
        amount = round(random.uniform(1200.0, 5000.0), 2) if random.random() < 0.15 else round(random.uniform(10.0, 350.0), 2)
        event = {
            "transaction_id": f"TXN_STREAM_{i:06d}",
            "user_id": f"USR_{random.randint(100, 150)}",
            "amount": amount,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": random.choice(['electronics', 'fashion', 'home', 'beauty']),
            "device_type": random.choice(['web', 'mobile_app'])
        }
        producer.send('ecommerce-transactions-stream', value=event)
        print(f"[ENVIADO] Transaction: {event['transaction_id']} | User: {event['user_id']} | Amount: ${amount}")
        i += 1
        time.sleep(1)
except KeyboardInterrupt:
    pass
