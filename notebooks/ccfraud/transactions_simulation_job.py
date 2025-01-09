import os
import random
import uuid
import json
import time

import datetime
import pandas as pd
import numpy as np

import hopsworks

from confluent_kafka import Producer

project = hopsworks.login()
fs = project.get_feature_store()

# kafka topic
KAFKA_TOPIC_NAME = f"{project.name}_real_time_live_transactions"
SCHEMA_NAME = "live_transactions_schema"

kafka_api = project.get_kafka_api()

# setup kafka producer

fs = project.get_feature_store()
kafka_config = fs._storage_connector_api.get_kafka_connector(fs.id, True).confluent_options()

print(kafka_config)
producer = Producer(kafka_config)

# read histrorical transactions from the batch feature group and produce records
fs = project.get_feature_store()
card_details = fs.get_feature_group(name="card_details", version=1).read()
merchant_details = fs.get_feature_group(name="merchant_details", version=1).read()

cust_card_dict = card_details["cc_num"].to_dict(orient='dict')
cc_nums = [i for i in cust_card_dict["cc_num"].keys()]
merchant_ids = merchant_details.to_list()

counter = 0  # Initialize a counter for demonstration

def generate_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

while True:
    # Print 10 values in the current batch
    for i in range(10):
        
        record = {'t_id': str(uuid.uuid4()),
                  'event_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                  'cc_num': str(random.choice(cc_nums)),
                  'merchant_id': str(random.choice(merchant_ids)),
                  'amount': np.random.gamma(2, 100, 1).clip(0.01, 10000).round(2)[0],
                  'ip_addr': generate_ip(),
                  'card_present': random.choices([True, False], weights=[66, 34], k=1)[0]
                 }        

        producer.produce(KAFKA_TOPIC_NAME, json.dumps(record))
    
    counter += 10

    if counter % 10 == 0:
        producer.flush()
        print(f'Finished sending index {counter}')
        
    # Wait for 10 second before the next batch
    time.sleep(10)