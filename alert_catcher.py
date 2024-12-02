from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Назва топіку
topic_names = ['alerts_rudyi']

# Підписка на тему
consumer.subscribe(topic_names)

print(f"Subscribed to topic '{topic_names}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(
            f"Received alert: {message.value} with key: {message.key}, partition {message.partition}")

except KeyboardInterrupt:
    print("Script stopped manually.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
