from pymongo import MongoClient
from as5_cfg import mongo

# Establish a connection to the MongoDB database
client = MongoClient(username=mongo['username'], password=mongo['password'])
db = client[mongo['database']]


def print_documents_in_collection(collection_name):
    """
    Print documents in the specified MongoDB collection.

    :param collection_name: The name of the MongoDB collection to print the documents from.
    """
    cursor = db[collection_name].find()
    for document in cursor:
        print(document)


# Print the documents from the MongoDB collections
print_documents_in_collection('woba')
print_documents_in_collection('halloffame')
print_documents_in_collection('batting')
print_documents_in_collection('appearances')
