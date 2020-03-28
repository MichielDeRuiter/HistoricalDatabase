from bson import ObjectId
from pymongo import MongoClient

client = MongoClient()
col = client.test.test

# insert document
# print(col.insert_one({'name': 'Foo', 'age': 1337}).inserted_id)

# update document
# doc = col.find_one_and_update(
#     {'_id': ObjectId('5e7bc3944edccf2c7ac9b846')},
#     {'$set': {'name': 'Bar', 'age': 1340}}, upsert=True)

# delete field
# doc = col.find_one_and_update(
#     {'_id': ObjectId('5e7bc3944edccf2c7ac9b846')},
#     {'$unset': {'age': ''}}, upsert=True)

# delete document
col.delete_one({'_id': ObjectId('5e7eaae1e988123fd75b46ae')})
