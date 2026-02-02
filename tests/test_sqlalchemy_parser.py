import unittest

from parser.sqlalchemy import SQLAlchemyFilterParser, SQLAlchemyJsonFilterParser
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MockModel(Base):
    __tablename__ = "mock_model"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    data = Column(JSONB)


class TestSQLAlchemyParsers(unittest.TestCase):
    def test_basic_parser(self):
        parser = SQLAlchemyFilterParser(MockModel)

        # Test eq
        expr = parser.create_filter("name eq 'John'")
        self.assertEqual(str(expr), "mock_model.name = :name_1")

        # Test AND/gt/lt
        expr = parser.create_filter("age gt 20 AND age lt 50")
        self.assertIn("mock_model.age > :age_1", str(expr))
        self.assertIn("mock_model.age < :age_2", str(expr))

    def test_json_parser_nested_search(self):
        parser = SQLAlchemyJsonFilterParser(MockModel)

        # Test nested JSON path: data.profile.name eq 'John'
        # This should map to MockModel.data['profile']['name'].astext
        expr = parser.create_filter("data.profile.name eq 'John'")

        from sqlalchemy.dialects import postgresql
        expr_str = str(expr.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

        # Exact output format from current SQLAlchemy version
        self.assertEqual(expr_str, "((mock_model.data['profile']) ->> 'name') = 'John'")

    def test_json_parser_integer_comparison(self):
        parser = SQLAlchemyJsonFilterParser(MockModel)

        # Test nested JSON path with integer: data.age gt 25
        expr = parser.create_filter("data.age gt 25")

        from sqlalchemy.dialects import postgresql
        expr_str = str(expr.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

        # Exact output format from current SQLAlchemy version
        self.assertEqual(expr_str, "(mock_model.data ->> 'age') > 25")

    def test_add_filter(self):
        parser = SQLAlchemyFilterParser(MockModel)
        stmt = select(MockModel)
        stmt_filtered = parser.add_filter("age gt 30", stmt)

        self.assertIn("WHERE mock_model.age > :age_1", str(stmt_filtered))


if __name__ == "__main__":
    unittest.main()
