#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
from kafka import KafkaConsumer

class StockPriceConsumer:
    def __init__(self, kafka_server, consumer_group):
        """
        Initialize the StockPriceConsumer with the given Kafka server and consumer group.

        """
        self.kafka_server = kafka_server
        self.consumer_group = consumer_group
        self.consumer = None

    def setup_consumer(self, topics):
        """
        Setup the Kafka consumer to subscribe to the given topics.

        """
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=[self.kafka_server],
            group_id=self.consumer_group,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Consumer group '{self.consumer_group}' started, subscribed to topics: {topics}")

    def consume_messages(self):
        """
        Consume messages from the subscribed topics.
        """
        if not self.consumer:
            raise RuntimeError("Consumer not set up. Call setup_consumer first.")
        
        try:
            for message in self.consumer:
                print(f"Received message from topic {message.topic}: {message.value}")
        except KeyboardInterrupt:
            print("Consumer interrupted")
        finally:
            self.consumer.close()

def main():
    """
    Main function to run the StockPriceConsumer.
    """
    if len(sys.argv) < 3:
        print("Usage: python consumer.py <CONSUMER_GROUP> <TOPICS>")
        sys.exit(1)

    kafka_server = '10.0.0.152:9092'
    consumer_group = sys.argv[1]
    topics = sys.argv[2:]

    consumer = StockPriceConsumer(kafka_server, consumer_group)
    consumer.setup_consumer(topics)
    consumer.consume_messages()

if __name__ == "__main__":
    main()
