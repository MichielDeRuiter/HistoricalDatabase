from datetime import datetime

from bson import ObjectId
from pymongo import MongoClient

main_client = MongoClient()
history_collection = main_client.test.history


def nearest(items, ts):
    return min([i for i in items if i < ts], key=lambda x: abs(x - ts))


# TODO: Reconstruct object to timestamp
def restore_object(object_id, timestamp):
    historical_object = history_collection.find_one({'_id': ObjectId(object_id)})
    timestamp = datetime.fromisoformat(timestamp)
    id = historical_object['_id']
    del historical_object['_id']

    restored_object = {}

    for k, v in historical_object.items():
        ts_list = []
        for ts in v:
            ts_list.append(ts['timestamp'])
        ts_list = [datetime.fromisoformat(ts) for ts in ts_list]
        n = nearest(ts_list, timestamp)
        restored_object[k] = next((item for item in v if item['timestamp'] == str(nearest(ts_list, timestamp))), None)['value']

    restored_object['_id'] = id

    return restored_object

# TODO: Reconstruct collection to timestamp
if __name__ == '__main__':

    # execute only if run as a script
    print(restore_object('5e7bc3944edccf2c7ac9b846', '2020-03-25 21:48:30.962634'))