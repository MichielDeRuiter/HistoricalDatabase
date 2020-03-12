from bson import ObjectId
from pymongo import MongoClient, ReadPreference
import os
import pymongo
from bson.json_util import dumps
import json

client = MongoClient()
client2 = MongoClient('localhost:27017', replicaset='rs0', read_preference=ReadPreference.PRIMARY)
main_collection = client2.test.test
history_collection = client.test.history

# print(client.test_database)
# print(client2.test_database)

change_stream = main_collection.watch()
for change in change_stream:
    print(dumps(change))
    print('')  # for readability only

    # insert
    # print(change["fullDocument"])
    # print(change["fullDocument"]["_id"])
    # for k, v in change["fullDocument"].items():
    #     print(k, v)

    # update
    for k, v in change["updateDescription"]["updatedFields"].items():
        print(k, v)

    print(change["documentKey"])


    # history_collection.find_one_and_update(
    #     {"_id": ObjectId(change["fullDocument"]["_id"])},
    #     {"$set":
    #          {"some field": "OBJECTROCKET ROCKS3!!"}
    #      }, upsert=True
    # )
    # history_collection.insert_one(change["fullDocument"])





# client.test.test.insert_one({"hello": "world"}).inserted_id
