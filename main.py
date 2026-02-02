from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select

from parser.dict_data import DictFilterParser
from parser.mongodb import MongoDBFilterParser
import motor.motor_asyncio
import asyncio
from actions import Action
from parser.sqlalchemy import SQLAlchemyFilterParser
from parser.py_ast_dict import DictASTFilterParser

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
    parser = SQLAlchemyFilterParser(Action)

    async with session_maker() as session:
        query = await session.execute(parser.add_filter("type eq 'TRANSFER'", select(Action)))
        result = query.unique().scalars()

        for record in result:
            print(record.name)

def fetch_items():
    items = [
        {"field1": 2, "name": "alpha", "tags": ["x", "y"]},
        {"field1": 4, "name": "beta", "tags": []},
        {"field1": 18, "name": "alphabet", "tags": ["z"]},
        {"field1": 20, "name": "gamma", "tags": ["y"]},
        {"field1": 30, "name": "delta", "nested": {"value": 10}},
    ]

    parser = DictASTFilterParser()

    # basic numeric
    print(parser.apply_filter("field1 gt 5 AND field1 lt 25", items))
    # string funcs/operators
    print(parser.apply_filter("name startswith 'alp' OR name like 'mm'", items))
    # membership: value in list literal
    print(parser.apply_filter("name in ['alpha','delta']", items))
    # sequences: has / contains
    print(parser.apply_filter("tags has 'y'", items))
    # dotted paths
    print(parser.apply_filter("nested.value eq 10", items))

async def main():
    await fetch_documents()
    await fetch_records()
    fetch_items()

asyncio.run(main())
