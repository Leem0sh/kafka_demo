import json

from kafka import KafkaProducer

hostname = "localhost"
port = 9092
topic_name = "10_parti_topic"

producer = KafkaProducer(
    api_version=(2, 6),
    bootstrap_servers=hostname + ":" + str(port),
    value_serializer=lambda
        v: json.dumps(v).encode('ascii'),
    key_serializer=lambda
        v: json.dumps(v).encode('ascii')
)
for x in range(1000):
    producer.send(
        topic_name,
        key={"id": 3},
        value={"name": " This is me", "pizza": f"Yummz {x}"}
    )

    producer.flush()
