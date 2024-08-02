#!/usr/bin/env python3

# Tiingo API token: bb69ec73112065aaef6d0bdddffc917f317cbe1b
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

    def parse_message(self, message):
        """
        Parse the received WebSocket message and return the structured data.
        """
        data = json.loads(message)
        if 'messageType' in data and data['messageType'] == 'A':  # Actual data message
            if 'data' in data:
                for ticker_data in data['data']:
                    ticker = ticker_data[3]  # Ticker
                    price_data = {
                        "messageType": ticker_data[0],  # Update Message Type
                        "timestamp": ticker_data[1],  # Date
                        "lastPrice": ticker_data[9] if ticker_data[0] == 'T' else None,  # Last Price
                        "lastSize": ticker_data[10] if ticker_data[0] == 'T' else None,  # Last Size
                        "bidSize": ticker_data[4] if ticker_data[0] == 'Q' else None,  # Bid Size
                        "bidPrice": ticker_data[5] if ticker_data[0] == 'Q' else None,  # Bid Price
                        "askPrice": ticker_data[7] if ticker_data[0] == 'Q' else None,  # Ask Price
                        "askSize": ticker_data[8] if ticker_data[0] == 'Q' else None,  # Ask Size
                        "afterHours": ticker_data[12]  # After Hours
                    }
                    return ticker, price_data
        return None, None

    def send_message(self, ticker, price_data):
        """
        Send the parsed message to the Kafka server.
        """
        if ticker and price_data:
            self.producer.send(ticker, value=price_data)
            print(f"Topic: {ticker} ==> Message: {price_data}")
            print("Message sent!")

    def on_message(self, ws, message):
        """
        Callback function when a message is received from the WebSocket.
        """
        ticker, price_data = self.parse_message(message)
        self.send_message(ticker, price_data)

    def on_error(self, ws, error):
        """
        Callback function when an error occurs.
        """
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Callback function when the WebSocket is closed.
        """
        print(f"WebSocket closed with status code: {close_status_code}, message: {close_msg}")

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

    def start_websocket(self):
        """
        Establish the WebSocket connection and start receiving messages.
        """
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp("wss://api.tiingo.com/iex",
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        while not self.stop_event.is_set():
            self.ws.run_forever()
            time.sleep(1)  # Avoid rapid reconnection causing too many requests

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
        print("Usage: python producer_v3.py <TICKER1> [<TICKER2> ...]")
        sys.exit(1)

    api_token = 'ybb69ec73112065aaef6d0bdddffc917f317cbe1b'  # Replace with your Tiingo API token
    tickers = sys.argv[1:]
    kafka_server = '10.0.0.152:9092'
    
    producer = StockPriceProducer(api_token, kafka_server, tickers)
    try:
        producer.start_websocket()
    except KeyboardInterrupt:
        producer.stop()
    finally:
        producer.stop()

if __name__ == "__main__":
    main()

