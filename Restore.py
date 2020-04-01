from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient

main_client = MongoClient()
tracked_collection = main_client.example_db.tracked_collection
historical_collection = main_client.example_db.historical_collection
db = main_client.test

def nearest(timestamps, timestamp):
    """
    Find the nearest timestamp before a certain timestamp, given a list of timestamps

    :param timestamps: A list of timestamps
    :param timestamp: The specified timestamp
    :return: Returns the nearest timestamp
    """
    return min([i for i in timestamps if i < timestamp], key=lambda x: abs(x - timestamp))


def restore_object(object_id, timestamp):
    """
    Restores a specific object to a point in time specified by a timestamp.
    If the historical object is marked as deleted, delete the object.
    If the last field of a the historical object contains 'deleted', delete it, otherwise include the field.

    :param object_id: The ID of the object that will be restored.
    :param timestamp: The timestamp on which the restoration is based.
    :return: The restored object.
    """
    historical_object = historical_collection.find_one({'_id': ObjectId(object_id)})
    timestamp = datetime.fromisoformat(timestamp)
    id = historical_object['_id']
    del historical_object['_id']
    restored_object = {}

    try:
        deleted_timestamp = datetime.fromisoformat(historical_object['deleted_timestamp'])
    except KeyError:
        deleted_timestamp = datetime.fromisoformat('9999-01-01 00:00:00.000000')

    if timestamp >= deleted_timestamp:
        print('Object {} deleted.'.format(object_id))
    else:
        try:
            del historical_object['deleted_timestamp']
        except KeyError:
            pass
        for k, v in historical_object.items():
            try:
                if 'deleted' in v[-1] and datetime.fromisoformat(v[-1]['timestamp']) >= deleted_timestamp:
                    pass
                else:
                    ts_list = []
                    for ts in v:
                        ts_list.append(ts['timestamp'])
                    ts_list = [datetime.fromisoformat(ts) for ts in ts_list]
                    try:
                        restored_object[k] = next((item for item in v if item['timestamp'] == str(nearest(ts_list, timestamp))), None)['value']
                    except ValueError:
                        restored_object = None
            except KeyError:
                pass
        try:
            restored_object['_id'] = id
        except TypeError:
            pass
        return restored_object


def restore_collection(timestamp):
    """
    Restores the tracked collection to a point in time specified by a timestamp.
    :param timestamp: The timestamp on which the restoration is based.
    :return: The restored collection.
    """
    restored_collection = []

    for document in tracked_collection.find():
        restored_object = restore_object(document['_id'], timestamp)
        print(restored_object)
        restored_collection.append(restored_object)

    return restored_collection

# TODO: Reconstruct collection to timestamp
if __name__ == '__main__':
    print(restore_object('5e850177e826f5e496c80b96', '2020-04-01 23:03:15.962634'))
