from bson import ObjectId
from pymongo import MongoClient

client = MongoClient()
tracked_collection = client.example_db.tracked_collection

# create object
# print(tracked_collection.insert_one({'name': 'Foo', 'age': 1337}).inserted_id)

# update object
# doc = tracked_collection.find_one_and_update(
#     {'_id': ObjectId('5e850e8ff77ffbe9b561c9c0')},
#     {'$set': {'name': 'Bar'}}, upsert=True)

# delete field
# doc = tracked_collection.find_one_and_update(
#     {'_id': ObjectId('5e850177e826f5e496c80b96')},
#     {'$unset': {'age': ''}}, upsert=True)

# delete object
tracked_collection.delete_one({'_id': ObjectId('5e850e8ff77ffbe9b561c9c0')})