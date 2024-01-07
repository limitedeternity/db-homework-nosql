from argparse import ArgumentParser
import asyncio
from contextlib import closing
from datetime import timezone
import functools
import operator
from pathlib import Path
import platform
from typing import Callable, TypedDict, TypeVar
import uuid

from aiokafka import AIOKafkaProducer
from asyncstdlib.itertools import _repeat as arepeat
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
    connect_uri = f"mongodb://mongodb:{env.get('MONGODB_PASSWORD')}@host.docker.internal:27017/?authSource=mongodb&directConnection=true"

    return AsyncIOMotorClient(connect_uri, server_api=ServerApi("1"), connect=False)


def create_producer():
    return AIOKafkaProducer(
        bootstrap_servers="host.docker.internal:10000",
        transactional_id=str(uuid.uuid4()),
        compression_type="snappy",
    )


def timestamp_ms(dt):
    ts = dt.replace(tzinfo=timezone.utc).timestamp()

    return int(ts * 1000.0)


async def main(args):
    with closing(create_motor()) as client:
        collection = client.mongodb[args.collection]

        producer = create_producer()
        await producer.start()

        try:
            with tqdm(arepeat(1), unit=" docs") as it:
                sort_on = [
                    ("event_timestamp", pymongo.ASCENDING),
                    ("trip_id", pymongo.ASCENDING),
                ]

                async with await client.start_session() as session:
                    async for _ in it:
                        async with session.start_transaction():
                            document = await collection.find_one_and_delete(
                                {},
                                projection={"_id": False},
                                sort=sort_on,
                            )

                            if not document:
                                break

                            async with producer.transaction():
                                await producer.send_and_wait(
                                    "nyc-taxi-events",
                                    key=document["trip_id"].binary,
                                    value=json_util.dumps(document).encode(),
                                    timestamp_ms=timestamp_ms(
                                        document["event_timestamp"]
                                    ),
                                )

        finally:
            await producer.stop()


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = ArgumentParser()
    parser.add_argument(
        "--collection", default="EventStream", help="Name of the collection"
    )

    asyncio.run(main(parser.parse_args()))
