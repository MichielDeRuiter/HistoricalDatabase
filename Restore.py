from datetime import datetime

from bson import ObjectId
from pymongo import MongoClient

main_client = MongoClient()
history_collection = main_client.test.history


def nearest(items, pivot):
    return min([i for i in items if i < pivot], key=lambda x: abs(x - pivot))





# TODO: Reconstruct collection to timestamp
if __name__ == '__main__':
    timestamps = ['2020-03-19 20:58:34.962634', '2020-03-19 20:58:47.551973']
    timestamps = [datetime.fromisoformat(date) for date in timestamps]

    print(timestamps)

    dt = datetime.fromisoformat('2020-03-19 20:58:42.962634')
    # print(dt)

    print(nearest(timestamps, dt))

    print()

