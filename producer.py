from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = str(uuid.uuid4())
# Назва топіку
topic_name = 'building_sensors_rudyi'

for i in range(1000):
    # Відправлення повідомлення в топік
    try:
        temperature = round(random.uniform(15, 55), 2)
        humidity = round(random.uniform(15, 85), 2)
        timestamp = time.time()

        # Формування повідомлення
        data = {
            "timestamp": timestamp,
            "temperature": temperature,
            "humidity": humidity
        }

        # Відправка повідомлення в Kafka
        producer.send(topic_name, key=sensor_id, value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(1)
    
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer
