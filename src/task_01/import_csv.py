import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
import functools
import operator
from pathlib import Path
import platform
from typing import Callable, Optional, TypedDict, TypeVar

from aiocsv import AsyncDictReader
import aiofiles
from dateutil.parser import parse as parse_datetime
from decouple import Config, RepositoryEnv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from pydantic import BaseModel, NonNegativeInt, PlainValidator, StringConstraints
from tqdm.asyncio import tqdm
from typing_extensions import Annotated


_T = TypeVar("_T")


def repeat_access(init: _T, attr: str, times: int) -> _T:
    accessor: Callable[[_T], _T] = operator.attrgetter(attr)

    return functools.reduce(lambda acc, _: accessor(acc), range(times), init)


class GlobalContext(TypedDict):
    root_dir: Path
    motor_client: ContextVar[Optional[AsyncIOMotorClient]]


CTX: GlobalContext = {
    "root_dir": repeat_access(Path(__file__).resolve(), "parent", 3),
    "motor_client": ContextVar("motor_client", default=None),
}


def create_motor():
    env = Config(RepositoryEnv(CTX["root_dir"] / ".env"))
    connect_uri = f"mongodb://mongodb:{env.get('MONGODB_PASSWORD')}@127.0.0.1:27017/?authSource=mongodb"

    return AsyncIOMotorClient(connect_uri, server_api=ServerApi("1"), connect=False)


@contextmanager
def scoped_context_value(context: ContextVar[_T], value: _T):
    init = context.set(value)

    try:
        yield

    finally:
        context.reset(init)


def needs_motor(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if CTX["motor_client"].get() is not None:
            return await func(*args, **kwargs)

        with scoped_context_value(CTX["motor_client"], create_motor()):
            return await func(*args, **kwargs)

    return wrapper


def acquire_motor():
    assert (client := CTX["motor_client"].get()) is not None

    return client


class TripData(BaseModel):
    _id: NonNegativeInt
    VendorID: int

    tpep_pickup_datetime: Annotated[
        datetime, PlainValidator(lambda val: parse_datetime(val, fuzzy=True))
    ]

    tpep_dropoff_datetime: Annotated[
        datetime, PlainValidator(lambda val: parse_datetime(val, fuzzy=True))
    ]

    passenger_count: NonNegativeInt
    trip_distance: float
    RatecodeID: int
    store_and_fwd_flag: Annotated[str, StringConstraints(min_length=1, max_length=1)]
    PULocationID: int
    DOLocationID: int
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float


async def async_enumerate(sequence, start=0):
    n = start

    async for elem in sequence:
        yield n, elem
        n += 1


@needs_motor
async def import_csv(csv_path: Path):
    client = acquire_motor()
    collection = client.mongodb.AllTrips

    assert (await collection.delete_many({})).acknowledged

    async with aiofiles.open(csv_path, mode="r", encoding="utf-8", newline="") as csv:
        reader = AsyncDictReader(csv)

        with tqdm(async_enumerate(reader), unit=" rows") as it:
            async for index, row in it:
                document = TripData(_id=index, **row)

                assert (await collection.insert_one(document.model_dump())).acknowledged

    print(f"{await collection.count_documents({})} documents processed")


@needs_motor
async def index_collection():
    client = acquire_motor()
    collection = client.mongodb.AllTrips

    await collection.create_index("tpep_pickup_datetime", background=True)
    await collection.create_index("tpep_dropoff_datetime", background=True)


async def main():
    csv_path = CTX["root_dir"] / "datasets" / "2018_Yellow_Taxi_Trip_Data_20231108.csv"

    await import_csv(csv_path)
    await index_collection()


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
