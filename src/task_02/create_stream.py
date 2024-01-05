from collections import OrderedDict
import functools
import operator
from pathlib import Path
from typing import Callable, TypedDict, TypeVar

from decouple import Config, RepositoryEnv
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


_T = TypeVar("_T")


def repeat_access(init: _T, attr: str, times: int) -> _T:
    accessor: Callable[[_T], _T] = operator.attrgetter(attr)

    return functools.reduce(lambda acc, _: accessor(acc), range(times), init)


class GlobalContext(TypedDict):
    root_dir: Path


CTX: GlobalContext = {
    "root_dir": repeat_access(Path(__file__).resolve(), "parent", 3),
}


def create_mongo():
    env = Config(RepositoryEnv(CTX["root_dir"] / ".env"))
    connect_uri = f"mongodb://mongodb:{env.get('MONGODB_PASSWORD')}@host.docker.internal:27017/?authSource=mongodb"

    return MongoClient(connect_uri, server_api=ServerApi("1"), connect=False)


def main():
    client = create_mongo()
    collection = client.mongodb.AllTrips
    (new_collection := client.mongodb.EventStream).drop()

    print(f"Performing aggregation on {collection.name} into {new_collection.name}")
    sort_on = OrderedDict([("event_timestamp", pymongo.ASCENDING), ("trip_id", pymongo.ASCENDING)])

    collection.aggregate(
        [
            {
                "$match": {
                    "$expr": {
                        "$gte": [
                            {
                                "$dateDiff": {
                                    "startDate": "$tpep_pickup_datetime",
                                    "endDate": "$tpep_dropoff_datetime",
                                    "unit": "second",
                                }
                            },
                            60,
                        ]
                    }
                }
            },
            {
                "$project": {
                    "data": [
                        {
                            "trip_id": "$_id",
                            "event_type": "start",
                            "event_timestamp": "$tpep_pickup_datetime",
                            "VendorID": "$VendorID",
                            "PULocationID": "$PULocationID",
                        },
                        {
                            "trip_id": "$_id",
                            "event_type": "end",
                            "event_timestamp": "$tpep_dropoff_datetime",
                            "passenger_count": "$passenger_count",
                            "trip_distance": "$trip_distance",
                            "RatecodeID": "$RatecodeID",
                            "store_and_fwd_flag": "$store_and_fwd_flag",
                            "DOLocationID": "$DOLocationID",
                            "payment_type": "$payment_type",
                            "fare_amount": "$fare_amount",
                            "extra": "$extra",
                            "mta_tax": "$mta_tax",
                            "tip_amount": "$tip_amount",
                            "tolls_amount": "$tolls_amount",
                            "improvement_surcharge": "$improvement_surcharge",
                            "total_amount": "$total_amount",
                        },
                    ]
                }
            },
            {"$unwind": "$data"},
            {"$replaceRoot": {"newRoot": "$data"}},
            {"$sort": sort_on},
            {"$out": "EventStream"},
        ],
        allowDiskUse=True,
    )

    print(f"Total: {new_collection.count_documents({})} documents")

    new_collection.create_index(list(sort_on.items()), background=True)


if __name__ == "__main__":
    main()
