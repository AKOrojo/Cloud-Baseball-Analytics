import csv
from pymongo import MongoClient
from as5_cfg import mongo

# Establish a connection to the MongoDB database
client = MongoClient(username=mongo['username'], password=mongo['password'])
db = client[mongo['database']]


def store_csv_to_mongodb(csv_path, collection_name):
    """
    Read the CSV file and store its data into the specified MongoDB collection.

    :param csv_path: The path to the CSV file.
    :param collection_name: The name of the MongoDB collection to store the data.
    """
    with open(csv_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            db[collection_name].insert_one(row)


# Store the CSV data in MongoDB collections
store_csv_to_mongodb('csv/Woba.csv', 'woba')
store_csv_to_mongodb('csv/HallOfFame.csv', 'halloffame')
store_csv_to_mongodb('csv/Batting.csv', 'batting')
store_csv_to_mongodb('csv/Appearances.csv', 'appearances')
