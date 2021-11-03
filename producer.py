import json

from kafka import KafkaProducer

hostname = "localhost"
port = 9092
topic_name = "t1"

producer = KafkaProducer(
    api_version=(2, 6),
    bootstrap_servers=hostname + ":" + str(port),
    value_serializer=lambda
        v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda
        v: json.dumps(v).encode('utf-8')
)
for x in range(500):
    producer.send(
        topic_name,
        key=None,
        value={"name": "", "key": f"value {x}"}
    )

    producer.flush()
