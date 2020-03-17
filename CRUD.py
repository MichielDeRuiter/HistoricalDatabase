import pymongo
from bson import ObjectId
from pymongo import MongoClient

client = MongoClient('localhost:27017')
col = client.test.test

# insert
print(col.insert_one({"name": "John", "age": 30}).inserted_id)

# update
# doc = col.find_one_and_update(
#     {"_id" : ObjectId("5e6ba6fe72c4f8d47409eaba")},
#     {"$set":
#         {"name":"John", "age": 47}
#     },upsert=True
# )

# delete
# col.delete_one({'_id': ObjectId("5e62aa67ea11412666fc3502")})
