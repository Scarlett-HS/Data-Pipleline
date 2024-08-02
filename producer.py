#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import kafka_errors

def read_csv_to_list_of_dict(file_path):
    df = pd.read_csv(file_path)
    df = df.fillna('')
    records_in_list_of_dict = df.to_dict(orient='records')
    return records_in_list_of_dict

def produce_messages(producer, topic, messages):
    for message in messages:
        # key = message['Date'].encode('utf-8')  # Use date as key
        producer.send(topic, value = message)
        print(f"Topic: {topic} ==> Message: {message}")
        print("Message sent!")
        producer.flush()
        time.sleep(0.5)

def main(company_symbol):
    kafka_server = 'localhost:9092'
    file_path = f"{company_symbol}_historical_data.csv"

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Read the content of CSV file as list[dict[Hashable, Any]] and send messages
    messages = read_csv_to_list_of_dict(file_path)
    produce_messages(producer, company_symbol, messages)

    producer.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python producer.py <COMPANY_SYMBOL>")
        sys.exit(1)
    
    company_symbol = sys.argv[1]
    main(company_symbol)
