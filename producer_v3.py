#!/usr/bin/env python3

import sys
import time
import json
import websocket
from kafka import KafkaProducer
from threading import Event

class StockPriceProducer:
    def __init__(self, api_token, kafka_server, tickers):
        """
        Initialize the StockPriceProducer with the given API token, Kafka server, and tickers.

        :param api_token: Tiingo API token
        :param kafka_server: Kafka server address
        :param tickers: List of stock ticker symbols
        """
        self.api_token = api_token
        self.kafka_server = kafka_server
        self.tickers = tickers
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.ws = None
        self.stop_event = Event()

    def on_message(self, ws, message):
        """
        Callback function when a message is received from the WebSocket.
        """
        data = json.loads(message)
        if 'data' in data:
            for ticker_data in data['data']:
                ticker = ticker_data['ticker']
                price_data = {
                    "timestamp": ticker_data['date'],
                    "lastPrice": ticker_data.get('last', None),
                    "lastSize": ticker_data.get('lastSize', None),
                    "bidSize": ticker_data.get('bidSize', None),
                    "bidPrice": ticker_data.get('bidPrice', None),
                    "askPrice": ticker_data.get('askPrice', None),
                    "askSize": ticker_data.get('askSize', None),
                    "afterHours": ticker_data.get('afterHours', None)
                }
                self.producer.send(ticker, value=price_data)
                print(f"Topic: {ticker} ==> Message: {price_data}")
                print("Message sent!")

    def on_error(self, ws, error):
        """
        Callback function when an error occurs.
        """
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Callback function when the WebSocket is closed.
        """
        print("WebSocket closed")

    def on_open(self, ws):
        """
        Callback function when the WebSocket is opened.
        """
        print("WebSocket connection opened")
        subscribe_message = {
            "eventName": "subscribe",
            "authorization": self.api_token,
            "eventData": {
                "thresholdLevel": 5,
                "tickers": self.tickers
            }
        }
        ws.send(json.dumps(subscribe_message))

    def produce_messages(self):
        """
        Establish the WebSocket connection and start producing messages.
        """
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp("wss://api.tiingo.com/iex",
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        while not self.stop_event.is_set():
            self.ws.run_forever()
            time.sleep(90)

    def stop(self):
        """
        Stop the WebSocket connection and the message production.
        """
        self.stop_event.set()
        if self.ws:
            self.ws.close()

def main():
    """
    Main function to run the StockPriceProducer.
    """
    if len(sys.argv) < 2:
        print("Usage: python producer_v2.py <TICKER1> [<TICKER2> ...]")
        sys.exit(1)

    api_token = 'bb69ec73112065aaef6d0bdddffc917f317cbe1b'  # Replace with your Tiingo API token
    tickers = sys.argv[1:]
    kafka_server = '10.0.0.152:9092'
    
    producer = StockPriceProducer(api_token, kafka_server, tickers)
    try:
        producer.produce_messages()
    except KeyboardInterrupt:
        producer.stop()

if __name__ == "__main__":
    main()

