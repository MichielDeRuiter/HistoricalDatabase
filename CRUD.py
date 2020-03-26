from bson import ObjectId
from pymongo import MongoClient

client = MongoClient()
col = client.test.test

# insert document
# print(col.insert_one({'name': 'Foo', 'age': 1337}).inserted_id)

# update document
doc = col.find_one_and_update(
    {'_id': ObjectId('5e7bc3944edccf2c7ac9b846')},
    {'$set': {'name': 'Bar', 'age': 1338}}, upsert=True)

# delete field
# doc = col.find_one_and_update(
#     {'_id': ObjectId('5e72433e0e9954067cf6f974')},
#     {'$unset': {'aaa': ''}}, upsert=True)

# delete document
# col.delete_one({'_id': ObjectId('5e727e25d7cf7cd79a956561')})
