import pymongo
from bson import ObjectId
from pymongo import MongoClient

client = MongoClient('localhost:27017')
col = client.test.test

# insert
# print(col.insert_one({"Name": "John", "age": 23}).inserted_id)

# update
doc = col.find_one_and_update(
    {"_id" : ObjectId("5e6aa44836686f709d957c0c")},
    {"$set":
        {"age": 34}
    },upsert=True
)

# delete
# col.delete_one({'_id': ObjectId("5e62aa67ea11412666fc3502")})
