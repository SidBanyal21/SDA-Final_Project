import pandas as pd
import time
from kafka import KafkaProducer
import json
from pymongo import MongoClient



def read_live_match_update_from_csv(file_path):
    return pd.read_csv(file_path)

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print("Test")

def insert_into_mongo(client,collection,message):
    collection.insert_one(message)

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    live_match_update = read_live_match_update_from_csv('live_match_update.csv')
    client = MongoClient("mongodb+srv://pragyanchoudhury48:jWME3bl0vQdZKZQd@sda2.ryhnp.mongodb.net/")  # replace with your MongoDB connection string
    db = client['football']  # replace with your database name
    collection = db['ticker_football_producer']  # replace with your collection name

    print("Main Started")
    for index, row in live_match_update.iterrows():
        message = {
            'Event ID': row['Event ID'],
            'Match ID': row['Match ID'],
            'Event Type': row['Event Type'],
            'Time': row['Time'],
            'Description': row['Description'],
        }
        print("Iteration Started:",message)

        send_to_kafka(producer, 'stocktopica', message)
        insert_into_mongo(client,collection,message)
        time.sleep(1)  # Adjust the sleep time as needed
