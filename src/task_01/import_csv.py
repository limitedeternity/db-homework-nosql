import asyncio
from contextlib import contextmanager, closing
from contextvars import ContextVar
from datetime import datetime
import functools
import operator
from pathlib import Path
import platform
from typing import Callable, Optional, TypedDict, TypeVar

from aiocsv import AsyncDictReader
import aiofiles
from asyncstdlib.builtins import enumerate as aenumerate, map as amap
from asyncstdlib.itertools import batched as abatched
from bson import ObjectId
from dateutil.parser import parse as parse_datetime
from decouple import Config, RepositoryEnv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PlainValidator,
    StringConstraints,
)

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
    connect_uri = f"mongodb://mongodb:{env.get('MONGODB_PASSWORD')}@host.docker.internal:27017/?authSource=mongodb&directConnection=true"

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

        with closing(create_motor()) as motor, scoped_context_value(
            CTX["motor_client"], motor
        ):
            return await func(*args, **kwargs)

    return wrapper


def acquire_motor():
    assert (client := CTX["motor_client"].get()) is not None

    return client


class TripData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Annotated[ObjectId, Field(alias="_id")]
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


def create_oid(n: int, /) -> ObjectId:
    return ObjectId(f"{n:x}".zfill(24))


def uncurry(func):
    @functools.wraps(func)
    def wrapper(args=None, kwargs=None):
        if args is None:
            args = ()

        if kwargs is None:
            kwargs = {}

        return func(*args, **kwargs)

    return wrapper


@needs_motor
async def import_csv(csv_path: Path):
    client = acquire_motor()
    collection = client.mongodb.AllTrips

    async with aiofiles.open(csv_path, mode="r", encoding="utf-8", newline="") as csv:
        with tqdm(
            amap(
                uncurry(
                    lambda index, row: TripData(
                        _id=create_oid(index), **row
                    ).model_dump(by_alias=True)
                ),
                aenumerate(AsyncDictReader(csv)),
            ),
            unit=" rows",
        ) as it:
            async with await client.start_session() as session:
                async for document_batch in abatched(it, 1024):
                    async with session.start_transaction():
                        assert (
                            await collection.insert_many(
                                document_batch, ordered=False, session=session
                            )
                        ).acknowledged

                document_count = await collection.count_documents({}, session=session)

    print(f"Total: {document_count} documents")


@needs_motor
async def index_collection():
    client = acquire_motor()
    collection = client.mongodb.AllTrips

    await collection.create_index("tpep_pickup_datetime", background=True)
    await collection.create_index("tpep_dropoff_datetime", background=True)


@needs_motor
async def main():
    csv_path = CTX["root_dir"] / "datasets" / "2018_Yellow_Taxi_Trip_Data_20231108.csv"

    client = acquire_motor()
    collection = client.mongodb.AllTrips

    await collection.drop()
    await import_csv(csv_path)
    await index_collection()


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
