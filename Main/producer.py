from kafka import KafkaProducer

bootstrap_servers = "localhost:9092"
topic = "wiki-changes"

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


def send_data_Tokafka(message):
    try:
        producer.send(topic, value=message.encode("utf-8"))
        print(f"Produced: {message} to Kafka topic: {topic} - producer.py:13")
    except Exception as error:
        print(f"Error: {error} - producer.py:15")
