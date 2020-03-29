from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient

main_client = MongoClient()
history_collection = main_client.test.history


def nearest(timestamps, ts):
    """
    Find the nearest timestamp before a certain timestamp, given a list of timestamps

    :param timestamps: A list of timestamps
    :param ts: The specified timestamp
    :return: Returns the nearest timestamp
    """
    return min([i for i in timestamps if i < ts], key=lambda x: abs(x - ts))


# TODO: Reconstruct object to timestamp
def restore_object(object_id, ts):
    """
    Restores a specific object to a point in time specified by a timestamp.
    If the historical object is marked as deleted, delete the object.
    If the last field of a the historical object contains 'deleted', delete it, otherwise include the field.

    :param object_id: The ID of the object that will be restored
    :param ts: The timestamp on which the restoration is based
    :return:
    """
    historical_object = history_collection.find_one({'_id': ObjectId(object_id)})
    ts = datetime.fromisoformat(ts)
    id = historical_object['_id']
    del historical_object['_id']
    restored_object = {}

    if 'deleted_timestamp' in historical_object:
        print('Object {} deleted.'.format(object_id))
    else:
        for k, v in historical_object.items():
            if 'deleted' in v[-1]:
                pass
            else:
                ts_list = []
                for timestamp in v:
                    ts_list.append(timestamp['timestamp'])
                ts_list = [datetime.fromisoformat(ts) for ts in ts_list]
                restored_object[k] = next((item for item in v if item['timestamp'] == str(nearest(ts_list, ts))), None)['value']

        restored_object['_id'] = id
        return restored_object


# TODO: Reconstruct collection to timestamp
if __name__ == '__main__':
    print(restore_object('5e7bc3944edccf2c7ac9b846', '2020-03-25 21:48:30.962634'))
    # print(restore_object('5e7eaae1e988123fd75b46ae', '2020-03-25 21:48:30.962634'))
