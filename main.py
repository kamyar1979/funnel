import asyncio

import motor.motor_asyncio
from actions import Action
from parser.mongodb import MongoDBFilterParser
from parser.py_ast_dict import DictASTFilterParser
from parser.sqlalchemy import SQLAlchemyJsonFilterParser
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# Connect to MongoDB
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
db = client["bff"]
collection = db["calls"]

engine = create_async_engine("postgresql+asyncpg://bff@localhost:5432/bff", pool_size=20, max_overflow=20)
session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def fetch_documents():
    parser = MongoDBFilterParser()

    query = parser.create_filter("duration gt 20 AND duration lt 100 AND call_id endswith '100'")
    print(query)

    documents = await collection.find(query).to_list(10)
    for doc in documents:
        print(doc)


async def fetch_records():
    parser = SQLAlchemyJsonFilterParser(Action)

    async with session_maker() as session:
        query = await session.execute(parser.add_filter("type eq 'TRANSFER'", select(Action)))
        result = query.unique().scalars()

        for record in result:
            print(record.name)

def fetch_items():
    from tests.mock_data import MOCK_ITEMS
    items = MOCK_ITEMS

    parser = DictASTFilterParser()

    # basic numeric
    print(parser.apply_filter("priority eq 1", items))
    # string funcs/operators
    print(parser.apply_filter("name startswith 'Proj' OR name like 'Integration'", items))
    # membership: value in list literal
    print(parser.apply_filter("type in ['INTERNAL']", items))
    # sequences: has / contains
    print(parser.apply_filter("tags has 'urgent'", items))
    # dotted paths
    print(parser.apply_filter("metadata.settings.notifications eq true", items))

async def main():
    await fetch_documents()
    await fetch_records()
    fetch_items()

asyncio.run(main())
