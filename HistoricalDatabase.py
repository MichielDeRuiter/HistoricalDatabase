from datetime import time, datetime

import bson
from pymongo import MongoClient, ReadPreference
from bson.json_util import dumps

main_client = MongoClient()
replica_client = MongoClient('localhost:27017', replicaset='rs0', read_preference=ReadPreference.PRIMARY)
change_stream = replica_client.example_db.tracked_collection
historical_collection = main_client.example_db.historical_collection

changes = change_stream.watch()
for change in changes:
    print(dumps(change))
    print('')

    operation_type = change['operationType']

    # create: Add the timestamp and the value
    if operation_type == 'insert':
        hist = {}
        id = change['fullDocument']['_id']
        for k, v in change['fullDocument'].items():
            hist[k] = [{'timestamp': str(datetime.now()), 'value': v}]
        hist['_id'] = id
        historical_collection.insert_one(hist)

    # update: Add the timestamp and the value. Add 'deleted' for deleted fields.
    elif operation_type == 'update':
        hist = historical_collection.find_one({'_id': change['documentKey']['_id']})
        update = change['updateDescription']

        for k, v in hist.items():
            if type(v) != bson.objectid.ObjectId:
                for kk, vv in update['updatedFields'].items():
                    if k == kk:
                        hist[kk].append({'timestamp': str(datetime.now()), 'value': vv})
                for kk in update['removedFields']:
                    if k == kk:
                        hist[k].append({'timestamp': str(datetime.now()), 'deleted': True})

        historical_collection.find_one_and_update(
            {'_id': change['documentKey']['_id']},
            {'$set': hist}, upsert=True)

    # delete: Add the timestamp and the value. Add 'deleted' for deleted fields.
    elif operation_type == 'delete':
        hist = historical_collection.find_one({'_id': change['documentKey']['_id']})
        hist['deleted_timestamp'] = str(datetime.now())
        historical_collection.find_one_and_update(
            {'_id': change['documentKey']['_id']},
            {'$set': hist}, upsert=True)