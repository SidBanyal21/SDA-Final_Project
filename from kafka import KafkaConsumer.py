from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'stocktopica',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='football-consumer-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb+srv://pragyanchoudhury48:jWME3bl0vQdZKZQd@sda2.ryhnp.mongodb.net/')
db = mongo_client['football_data']
events_collection = db['events']

# Consumer processing loop
for message in consumer:
    data = message.value
    match_id = data['Match ID']
    event_type = data['Event Type']
    time = data['Time']
    description = data['Description']

    # Insert event data into MongoDB
    events_collection.insert_one({
        'match_id': match_id,
        'event_type': event_type,
        'time': time,
        'description': description
    })

    # Print alert for specific events
    if event_type == 'card':
        print(f"Alert! Card shown in match {match_id}")
        print(f"Description: {description}")
    elif event_type == 'goal':
        print(f"Alert! Goal scored in match {match_id}")
        # Extract goal scorer information from description (if available)
        try:
            goal_scorer = description.split("by ")[1].split(" ")[0]
            print(f"Goal scored by: {goal_scorer}")
        except IndexError:
            print("Could not extract goal scorer information.")
    elif event_type in ['foul', 'corner', 'free kick']:
        print(f"Alert! {event_type} in match {match_id}")
        print(f"Description: {description}")