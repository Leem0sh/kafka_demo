from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    api_version=(2, 6),
    bootstrap_servers="localhost:9092",
    client_id='test'
)

topic_list = []
topic_list.append(
    NewTopic(name="10_parti_topic", num_partitions=10, replication_factor=1)
)
admin_client.create_topics(new_topics=topic_list, validate_only=False)
