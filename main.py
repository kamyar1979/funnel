from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select

from parser.dict_data import DictFilterParser
from parser.mongodb import MongoDbFilterParser
import motor.motor_asyncio
import asyncio
from actions import Action
from parser.sqlalchemy import SqlAlchemyFilterParser

# Connect to MongoDB
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
db = client["bff"]
collection = db["calls"]

engine = create_async_engine("postgresql+asyncpg://bff@localhost:5432/bff", pool_size=20, max_overflow=20)
session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def fetch_documents():
    parser = MongoDbFilterParser()

    query = parser.create_filter("duration gt 20 AND duration lt 100 AND call_id endswith '100'")
    print(query)

    documents = await collection.find(query).to_list(10)
    for doc in documents:
        print(doc)


async def fetch_records():
    parser = SqlAlchemyFilterParser(Action)

    async with session_maker() as session:
        query = await session.execute(parser.add_filter("type eq 'TRANSFER'", select(Action)))
        result = query.unique().scalars()

        for record in result:
            print(record.name)

def fetch_items():
    parser = DictFilterParser()

    items = [
        {"field1": 2},
        {"field1": 4},
        {"field1": 18},
        {"field1": 20},
    ]

    print(parser.apply_filter("field1 gt 5", items))

fetch_items()

