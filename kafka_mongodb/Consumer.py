import pymongo 
from pymongo import MongoClient
import json
from kafka import KafkaConsumer

# Define the MongoDB connection string
connection_string = f"conn_string"

def main():
    consumer = KafkaConsumer('Reddit_app',
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    client = pymongo.MongoClient(connection_string) # Replace connection_string with your MongoDB Atlas connection string
    
    Reddit_db = client['Reddit_app']
    collection = Reddit_db['Batman'] # Replace your_collection_name with the desired collection name

    try:
        for message in consumer:
            data = message.value
            collection.insert_one(data)
            print("Message inserted into MongoDB:", data)
    except Exception as e:
        print("An error occurred:", e)
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    main()
