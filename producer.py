# !/usr/bin/env python

# Imports
from __future__ import print_function
from kafka import SimpleProducer, KafkaClient
from satori.rtm.client import make_client, SubscriptionMode
import sys
import threading
import json


# Satori vars
channel = "transportation"
endpoint = "wss://open-data.api.satori.com"
appkey = "b43E7E3B430f1aA491787f2bdC690898"

# Assign a topic
topic = 'transportation'


# Producer code
def satori_client():
    # connect to transportation data stream at satori
    with make_client(endpoint=endpoint, appkey=appkey) as client:
        print('Connected to channel: {}'. format(topic))

        mailbox = []
        got_message_event = threading.Event()

        class SubscriptionObserver(object):
            def on_subscription_data(self, data):
                for message in data['messages']:
                    mailbox.append(message)
                    got_message_event.set()

        # instantiate class
        subscription_observer = SubscriptionObserver()

        client.subscribe(
            channel,
            SubscriptionMode.SIMPLE,
            subscription_observer)

        if not got_message_event.wait(20):
            print('Message timeout exceeded.')
            sys.exit(1)

        # return json in mailbox object
        while mailbox:
            send_json(json.dumps(mailbox[-1], sort_keys=True).encode('utf-8'))


def send_json(data):
    kafka = KafkaClient('localhost:9092')

    # Connect to Kafka and send json
    producer = SimpleProducer(kafka)

    producer.send_messages(topic, data)

if __name__ == '__main__':
    # Get messages from satori client
    satori_client()




