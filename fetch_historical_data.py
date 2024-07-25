import yfinance as yf
import pandas as pd

# List of companies
companies = ['TSLA', 'NVDA', 'INTC', 'AMD', 'MU', 'QCOM', 'MRVL']

# Define date range
end_date = pd.Timestamp.now()
start_date = end_date - pd.DateOffset(years=1)

# Fetch and save historical data for each company
for company in companies:
    # Download historical data
    df = yf.download(company, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    
    # Save to CSV
    csv_path = f"{company}_historical_data.csv"
    df.to_csv(csv_path, index=True)
    print(f"Saved {company} historical data to {csv_path}")

print("All data fetched and saved.")

