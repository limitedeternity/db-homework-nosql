from argparse import ArgumentParser
import asyncio
from collections import OrderedDict
from contextlib import closing
from datetime import timezone
import functools
import operator
from pathlib import Path
from typing import Callable, TypedDict, TypeVar
import uuid

from aiokafka import AIOKafkaProducer
from bson import json_util
from decouple import Config, RepositoryEnv
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.server_api import ServerApi
from tqdm.asyncio import tqdm


_T = TypeVar("_T")


def repeat_access(init: _T, attr: str, times: int) -> _T:
    accessor: Callable[[_T], _T] = operator.attrgetter(attr)

    return functools.reduce(lambda acc, _: accessor(acc), range(times), init)


class GlobalContext(TypedDict):
    root_dir: Path


CTX: GlobalContext = {
    "root_dir": repeat_access(Path(__file__).resolve(), "parent", 3),
}


def create_motor():
    env = Config(RepositoryEnv(CTX["root_dir"] / ".env"))
    connect_uri = f"mongodb://mongodb:{env.get('MONGODB_PASSWORD')}@host.docker.internal:27017/?authSource=mongodb"

    return AsyncIOMotorClient(connect_uri, server_api=ServerApi("1"), connect=False)


def create_producer():
    return AIOKafkaProducer(
        bootstrap_servers="host.docker.internal:10000",
        transactional_id=str(uuid.uuid4()),
    )


async def async_count(start: int = 0, step: int = 1):
    n = start

    while True:
        yield n
        n += step


def timestamp_ms(dt):
    ts = dt.replace(tzinfo=timezone.utc).timestamp()

    return int(ts * 1000.0)


async def main(args):
    with closing(create_motor()) as client:
        collection = client.mongodb[args.collection]

        producer = create_producer()
        await producer.start()

        try:
            with tqdm(async_count(), unit=" docs") as it:
                sort_on = OrderedDict(
                    [
                        ("event_timestamp", pymongo.ASCENDING),
                        ("trip_id", pymongo.ASCENDING),
                    ]
                )

                async for _ in it:
                    document = await collection.find_one_and_delete(
                        {}, projection={"_id": False}, sort=list(sort_on.items())
                    )

                    if not document:
                        break

                    async with producer.transaction():
                        await producer.send_and_wait(
                            "nyc-taxi-events",
                            key=document["trip_id"].binary,
                            value=json_util.dumps(document).encode(),
                            timestamp_ms=timestamp_ms(document["event_timestamp"]),
                        )

        finally:
            await producer.stop()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--collection", default="EventStream", help="Name of the collection"
    )

    asyncio.run(main(parser.parse_args()))
