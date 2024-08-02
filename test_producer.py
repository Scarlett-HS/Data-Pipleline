import time
import pytest
import requests_mock
from producer_v2 import StockPriceProducer

@pytest.fixture
def producer():
    api_key = 'test_api_key'
    kafka_server = '10.0.0.152:9092'
    return StockPriceProducer(api_key, kafka_server)

def test_get_realtime_price(producer, requests_mock):
    ticker = 'AAPL'

    # Prepare multiple mock responses
    mock_responses = [
        {
            "Time Series (1min)": {
                "2023-07-19 16:00:00": {
                    "1. open": "150.00",
                    "2. high": "155.00",
                    "3. low": "149.00",
                    "4. close": "154.00",
                    "5. volume": "10000"
                }
            }
        },
        {
            "Time Series (1min)": {
                "2023-07-19 16:01:00": {
                    "1. open": "154.00",
                    "2. high": "156.00",
                    "3. low": "152.00",
                    "4. close": "155.00",
                    "5. volume": "20000"
                }
            }
        },
        {
            "Time Series (1min)": {
                "2023-07-19 16:02:00": {
                    "1. open": "155.00",
                    "2. high": "158.00",
                    "3. low": "154.00",
                    "4. close": "157.00",
                    "5. volume": "15000"
                }
            }
        }
    ]

    # Mock the API responses for each call and test them
    for i, mock_response in enumerate(mock_responses):
        requests_mock.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey=test_api_key', json = mock_response)
        
        price_data = producer.get_realtime_price(ticker)
        assert price_data is not None
        assert price_data['timestamp'] in mock_response["Time Series (1min)"]
        expected_data = mock_response["Time Series (1min)"][price_data['timestamp']]
        assert price_data['open'] == expected_data['1. open']
        assert price_data['high'] == expected_data['2. high']
        assert price_data['low'] == expected_data['3. low']
        assert price_data['close'] == expected_data['4. close']
        assert price_data['volume'] == expected_data['5. volume']

        # Print out which iteration we are on for clarity
        print(f"Test iteration {i+1}: Retrieved data {price_data}")

        # Wait for 1 second before the next call (simulate 1-minute interval without long wait during testing)
        time.sleep(1)

if __name__ == "__main__":
    pytest.main()
