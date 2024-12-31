import pandas as pd
import time
from kafka import KafkaProducer
import json
from pymongo import MongoClient



def read_railway_from_csv(file_path):
    return pd.read_csv(file_path)

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print("Test")

def insert_into_mongo(client,collection,message):
    collection.insert_one(message)

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    railway_update = read_railway_from_csv('Railway.csv')
    client = MongoClient("mongodb+srv://Siddhant:IYe0ZuDwSIStgY41@cluster1.puwyf.mongodb.net/")  # replace with your MongoDB connection string
    db = client['railway']  # replace with your database name
    collection = db['ticker_railway_producer']  # replace with your collection name

    print("Main Started")
    for index, row in railway_update.iterrows():
        message = {
            'Time': row['Time'],
            'Train_ID': row['Train_ID'],
            'Station': row['Station'],
            'Alert_Type': row['Alert_Type'],
            'Severity': row['Severity']
        }
        print("Iteration Started:",message)

        send_to_kafka(producer, 'stocktopic2', message)
        insert_into_mongo(client,collection,message)
        time.sleep(1)  # Adjust the sleep time as needed

if __name__ == "__main__":
    main()
