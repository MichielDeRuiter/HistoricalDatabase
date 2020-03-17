from datetime import time, datetime

import bson
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
    print('')

    # update
    try:
        # for k, v in change["updateDescription"]["updatedFields"].items():
        #     print(k, v)
        #
        # print(change["updateDescription"]["updatedFields"])
        # print(change["documentKey"])
        # print(change["documentKey"]["_id"])
        # print(history_collection.find_one({"_id": change["documentKey"]["_id"]}))

        try:
            dic = history_collection.find_one({"_id": change["documentKey"]["_id"]})
        except:
            dic = {}

        for k, v in history_collection.find_one({"_id": change["documentKey"]["_id"]}).items():
            for kk, vv in change["updateDescription"]["updatedFields"].items():
                if type(v) != bson.objectid.ObjectId:
                    if k == kk:
                        if type(v) != list:
                            dic[k] = [{'timestamp': str(datetime.now()), 'value': vv}]
                        else:
                            dic[k].append({'timestamp': str(datetime.now()), 'value': vv})

        history_collection.find_one_and_update(
            {"_id": change["documentKey"]["_id"]},
            {"$set":
                 dic
             }, upsert=True
        )
    except KeyError:
        dic = {}
        print(change["fullDocument"])
        for k, v in change["fullDocument"].items():
            dic[k] = [{'timestamp': str(datetime.now()), 'value': v}]
            print(dic)
        del dic['_id']
        print(dic)
        history_collection.insert_one(dic)


    # history_collection.find_one_and_update(
    #     {"_id": history_collection.find_one({"_id": change["documentKey"]["_id"]})},
    #     {"$set":
    #          {"age": 32}
    #      }, upsert=True
    # )

    # history_collection.insert_one(change["fullDocument"])

    # history_collection.find_one_and_update(
    #     {"_id": ObjectId(change["fullDocument"]["_id"])},
    #     {"$set":
    #          {"some field": "OBJECTROCKET ROCKS3!!"}
    #      }, upsert=True
    # )
    # history_collection.insert_one(change["fullDocument"])

# client.test.test.insert_one({"hello": "world"}).inserted_id
