#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import json
import requests
from kafka import KafkaProducer

class StockPriceProducer:
    def __init__(self, api_key, kafka_server):
        """
        Initialize the StockPriceProducer with the given API key and Kafka server.

        """
        self.api_key = api_key
        self.kafka_server = kafka_server
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def get_realtime_price(self, ticker):
        """
        Get the real-time price of the given ticker from Alpha Vantage.

        :param ticker: Stock ticker symbol
        :return: Dictionary containing timestamp, open, high, low, close, and volume
        """
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey={self.api_key}'
        response = requests.get(url)
        data = response.json()
        # print("data: \n", data)
        
        
        if "Time Series (1min)" in data:
            last_refreshed = list(data['Time Series (1min)'].keys())[0]
            price_data = data['Time Series (1min)'][last_refreshed]
            return {
                "timestamp": last_refreshed,
                "open": price_data['1. open'],
                "high": price_data['2. high'],
                "low": price_data['3. low'],
                "close": price_data['4. close'],
                "volume": price_data['5. volume']
            }
        else:
            print("Error in retrieving data")
            return None

    def produce_messages(self, ticker):
        """
        Produce messages to the Kafka topic corresponding to the given ticker.

        """
        while True:
            message = self.get_realtime_price(ticker)
            if message:
                self.producer.send(ticker, value = message)
                print(f"Topic: {ticker} ==> Message: {message}")
                print("Message sent!")
            else:
                print(f"Failed to retrieve data for {ticker}")
            time.sleep(60)  # Fetch data every minute

def main():
    """
    Main function to run the StockPriceProducer.
    """
    if len(sys.argv) != 2:
        print("Usage: python producer1.py <TICKER_SYMBOL>")
        sys.exit(1)

    ticker_symbol = sys.argv[1]
    api_key = '46RXGXIFZMI7QJZX'
    kafka_server = '10.0.0.152:9092'
    
    producer = StockPriceProducer(api_key, kafka_server)
    producer.produce_messages(ticker_symbol)

if __name__ == "__main__":
    main()

