import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate sample data
np.random.seed(0)
n_transactions = 100
credit_cards = [f"card_{i}" for i in range(1, 6)]
timestamps = [datetime.now() - timedelta(minutes=np.random.randint(0, 60)) for _ in range(n_transactions)]
amounts = np.random.uniform(10, 100, size=n_transactions)
cards = np.random.choice(credit_cards, size=n_transactions)

# Create DataFrame
data = pd.DataFrame({
    'timestamp': timestamps,
    'amount': amounts,
    'credit_card': cards
})

# Set the timestamp as the index
data = data.sort_values('timestamp').set_index('timestamp')

# Display the first few rows of the DataFrame
print("Sample transactions data:")
print(data.head())

# Calculate the sum of amounts in the last 10 minutes for each credit card
window_size = '10T'  # 10 minutes
aggregated_data = data.groupby('credit_card').rolling(window=window_size, closed='both').amount.sum().reset_index()

# Display the result
print("\nTotal amount spent in the last 10 minutes for each credit card:")
print(aggregated_data.head(10))
