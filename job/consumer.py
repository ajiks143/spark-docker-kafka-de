from json import loads
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'xapo2',
    bootstrap_servers=['localhost:19092'],
    auto_offset_reset='earliest',
    #enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    print(f"{message.value}")