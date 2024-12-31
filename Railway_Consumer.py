from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'stocktopic2',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='railway-consumer-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb+srv://Siddhant:IYe0ZuDwSIStgY41@cluster1.puwyf.mongodb.net/')
db = mongo_client['railway_data']
events_collection = db['alerts']

# Consumer processing loop
for message in consumer:
    data = message.value
    time = data['Time']
    train_id = data['Train_ID']
    station = data['Station']
    alert_type = data['Alert_Type']
    severity = data['Severity']

    # Insert event data into MongoDB
    events_collection.insert_one({
        'time': time,
        'train_id': train_id,
        'station': station,
        'alert_type': alert_type,
        'severity': severity
    })

    # Print alert for specific events
    if alert_type == 'Collision Risk':
        print(f"Alert! Collision risk is there and severity {severity}")
    elif alert_type == 'Fire Detection':
        print(f"Alert! Fire detected in the train and severity {severity}")
    elif alert_type in ['Medical Emergency', 'Track Obstruction', 'Power Failure','Maintenance Due']:
        print(f"Alert! {alert_type} is there and severity {severity}")
