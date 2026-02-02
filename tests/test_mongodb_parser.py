import unittest

from parser.mongodb import MongoDBFilterParser


class TestMongoDBFilterParser(unittest.TestCase):
    def setUp(self):
        self.parser = MongoDBFilterParser()

    def test_basic_ops(self):
        # eq
        query = self.parser.create_filter("field eq 10")
        self.assertEqual(query, {"field": {"$eq": 10}})

        # gt/lt
        query = self.parser.create_filter("duration gt 20 AND duration lt 100")
        self.assertEqual(query, {"$and": [{"duration": {"$gt": 20}}, {"duration": {"$lt": 100}}]})

    def test_string_ops(self):
        # startswith
        query = self.parser.create_filter("name startswith 'John'")
        self.assertEqual(query, {"name": {"$regex": "^John", "$options": "i"}})

        # endswith
        query = self.parser.create_filter("call_id endswith '100'")
        self.assertEqual(query, {"call_id": {"$regex": "100$", "$options": "i"}})

    def test_logical_ops(self):
        query = self.parser.create_filter("a eq 1 OR b eq 2")
        self.assertEqual(query, {"$or": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]})

    def test_collection_ops(self):
        # has
        query = self.parser.create_filter("tags has 'urgent'")
        self.assertEqual(query, {"tags": {"$elemMatch": {"$eq": "urgent"}}})


if __name__ == "__main__":
    unittest.main()
