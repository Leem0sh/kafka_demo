import json

from kafka import KafkaConsumer, KafkaProducer

hostname = "localhost"
port = 9092
topic_name = "t1"
topic_name2 = "t2"


consumer = KafkaConsumer(
    group_id="g1",
    client_id="client1",
    bootstrap_servers=hostname + ":" + str(port),
    value_deserializer=lambda
        v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda
        v: json.loads(v.decode('utf-8')),
    max_poll_records=10
)



consumer.subscribe(topics=[topic_name2])

for message in consumer:
    print(
        "%d:%d: k=%s v=%s" % (message.partition,
                              message.offset,
                              message.key,
                              message.value)
    )