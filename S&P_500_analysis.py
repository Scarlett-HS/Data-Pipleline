import pandas as pd

# Read the Excel file
file_path = 'S&P_500_historical_data.xlsx'
df = pd.read_excel(file_path)

# Calculate the average closing price over the past year
average_close = df['Close'].mean()

# Calculate the daily returns
df['Daily Return'] = df['Close'].pct_change()

# Calculate the average daily return
average_daily_return = df['Daily Return'].mean()

# Print the results
print(f"Average Closing Price: {average_close}")
print(f"Average Daily Return: {average_daily_return}")
