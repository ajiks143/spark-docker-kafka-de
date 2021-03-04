from time import sleep
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer
import requests
import sys

# API URL and Kafka config
URL =  "https://chain.api.btc.com/v3/block/latest/tx"
topic = "xapo"
producer = KafkaProducer(
        bootstrap_servers=['broker:9092'], 
        value_serializer=lambda x: dumps(x).encode('utf-8')
        )

# Getting API request
r = requests.get(url = URL)
response = r.json()
datasets = response['data']['list']

#Sending the data to Kafka Topic
print("Sending data to Kafka...")

if datasets is not None:
        for dataset in datasets:
                producer.send(topic, dataset)
else:
        print("Empty API response")
        sys.exit(-1)


