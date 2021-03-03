from time import sleep
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer
import requests

# API URL and Kafka config
URL =  "https://chain.api.btc.com/v3/block/latest/tx"
producer = KafkaProducer(
        bootstrap_servers=['localhost:19092'], 
        value_serializer=lambda x: dumps(x).encode('utf-8')
        )

# Getting API request
r = requests.get(url = URL)
response = r.json()
datasets = response['data']['list']

#Sending the data to Kafka Topic
for dataset in datasets:
    producer.send('xapo1', dataset)



