# Imports
from kafka import KafkaConsumer
import pandas as pd
from pandas.io.json import json_normalize
import json


# connect to Kafka server and pass the topic we want to consume
def kafkastream_to_df():
    consumer = KafkaConsumer('transportation', group_id='view', bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        json_data = json.loads(msg.value)
        json_parse = pd.io.json.json_normalize(json_data)
        print(json_parse)


if __name__ == '__main__':
    kafkastream_to_df()
