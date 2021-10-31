import json

from kafka import KafkaConsumer

hostname = "localhost"
port = 9092
topic_name = "10_parti_topic"

consumer = KafkaConsumer(
    group_id="hayo",
    client_id="client1",
    bootstrap_servers=hostname + ":" + str(port),
    value_deserializer=lambda
        v: json.loads(v.decode('ascii')),
    key_deserializer=lambda
        v: json.loads(v.decode('ascii')),
    max_poll_records=10
)

consumer.subscribe(topics=[topic_name])

for message in consumer:
    print(
        "%d:%d: k=%s v=%s" % (message.partition,
                              message.offset,
                              message.key,
                              message.value)
    )
