# fetch_logs.py (Running in a separate Command Prompt)
import csv
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import json
import pandas as pd
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'server_logs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',                  
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=3000 )

with open('live_data.csv', 'a', newline='') as f:
    writer = csv.writer(f)
    for message in consumer:
        # Write the incoming log to the CSV file instantly
        writer.writerow(message.value.values())