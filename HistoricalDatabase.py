from datetime import time, datetime

import bson
from pymongo import MongoClient, ReadPreference
from bson.json_util import dumps

main_client = MongoClient()
replica_client = MongoClient('localhost:27017', replicaset='rs0', read_preference=ReadPreference.PRIMARY)
change_stream = replica_client.test.test
history_collection = main_client.test.history

# print(main_client.test_database)
# print(replica_client.test_database)

changes = change_stream.watch()
for change in changes:
    print(dumps(change))
    print('')

    operation_type = change['operationType']

    if operation_type == 'insert':
        hist = {}
        id = change['fullDocument']['_id']
        for k, v in change['fullDocument'].items():
            hist[k] = [{'timestamp': str(datetime.now()), 'value': v}]
        hist['_id'] = id
        history_collection.insert_one(hist)

    elif operation_type == 'update':
    # update
        # for k, v in change['updateDescription']['updatedFields'].items():
        #     print(k, v)
        #
        # print(change['updateDescription']['updatedFields'])
        # print(change['documentKey'])
        # print(change['documentKey']['_id'])
        # print(history_collection.find_one({'_id': change['documentKey']['_id']}))
        # print(history_collection.find_one({'_id': change['documentKey']['_id']}).items())
        # print(change['updateDescription']['updatedFields'].items())

        hist = history_collection.find_one({'_id': change['documentKey']['_id']})
        update = change['updateDescription']

        for k, v in hist.items():
            if type(v) != bson.objectid.ObjectId:
                for kk, vv in update['updatedFields'].items():
                    if k == kk:
                        hist[kk].append({'timestamp': str(datetime.now()), 'value': vv})
                    # else:
                        # print(kk, vv)
                        # dic[kk] = [{'timestamp': str(datetime.now()), 'value': vv}]
                for kk in update['removedFields']:
                    if k == kk:
                        hist[k].append({'timestamp': str(datetime.now()), 'deleted': True})
                    # else:
                    #     dic[k] = [{'timestamp': str(datetime.now()), 'deleted': True}]
        # print(hist)

        history_collection.find_one_and_update(
            {'_id': change['documentKey']['_id']},
            {'$set': hist}, upsert=True)

    # TODO:
    # elif operation_type == 'replace':
    #     print('replace')
    elif operation_type == 'delete':
        hist = history_collection.find_one({'_id': change['documentKey']['_id']})
        hist['deleted_timestamp'] = datetime.now()
        history_collection.find_one_and_update(
            {'_id': change['documentKey']['_id']},
            {'$set': hist}, upsert=True)




    # history_collection.find_one_and_update(
    #     {'_id': history_collection.find_one({'_id': change['documentKey']['_id']})},
    #     {'$set':
    #          {'age': 32}
    #      }, upsert=True
    # )

    # history_collection.insert_one(change['fullDocument'])

    # history_collection.find_one_and_update(
    #     {'_id': ObjectId(change['fullDocument']['_id'])},
    #     {'$set':
    #          {'some field': 'OBJECTROCKET ROCKS3!!'}
    #      }, upsert=True
    # )
    # history_collection.insert_one(change['fullDocument'])

# client.test.test.insert_one({'hello': 'world'}).inserted_id
