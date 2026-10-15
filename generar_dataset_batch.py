import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

num_records = 10000
np.random.seed(42)

start_date = datetime(2026, 10, 1)
data = []

categories = ['electronics', 'fashion', 'home', 'beauty', 'sports']
devices = ['web', 'mobile_app', 'tablet']

for i in range(1, num_records + 1):
    trans_id = f"TXN_{i:06d}"
    user_id = f"USR_{random.randint(100, 500)}"
    amount = round(float(np.random.exponential(scale=50) + 5), 2)
    timestamp = start_date + timedelta(seconds=random.randint(0, 86400 * 5))
    category = random.choice(categories)
    device = random.choice(devices)
    
    is_fraud = 0
    if random.random() < 0.015 or amount > 800:
        is_fraud = 1
        if amount <= 800:
            amount = round(random.uniform(900, 5000), 2)

    data.append([trans_id, user_id, amount, timestamp.strftime("%Y-%m-%d %H:%M:%S"), category, device, is_fraud])

df = pd.DataFrame(data, columns=['transaction_id', 'user_id', 'amount', 'timestamp', 'category', 'device_type', 'is_fraud'])
df.to_csv('ecommerce_transactions.csv', index=False)
print("Dataset creado exitosamente: ecommerce_transactions.csv")
