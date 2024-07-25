#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
from kafka import KafkaConsumer

def main(consumer_group, consumer_name):
	kafka_server = 'localhost:9092'
	topics = ['AMD', 'INTC', 'MRVL', 'MU', 'NVDA', 'QCOM', 'TSLA']

	# Initialize Kafka consumer
	consumer = KafkaConsumer(
		
		bootstrap_servers=[kafka_server],
		group_id = consumer_group,
		auto_offset_reset = 'earliest',
		enable_auto_commit = True,
		value_deserializer = lambda v: json.loads(v.decode('utf-8')),
		# key_deserializer = lambda k: k.decode('utf-8')
	)

	# Subscribe to topics
	consumer.subscribe(topics)

	print(f"Consumer {consumer_name} started in Group {consumer_group}")
	
	for message in consumer:
		print(f"Topic: {message.topic} ==> Message: {message.value}")
		print(f'received by Consumer {consumer_name} from Group {consumer_group}.')
  
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: python consumer.py <CONSUMER_GROUP> <CONSUMER_NAME>")
		sys.exit(1)
	
	consumer_group = sys.argv[1]
	consumer_name = sys.argv[2]
	main(consumer_group, consumer_name)
