import unittest
from datetime import datetime, time

from parser import FilterParser


class MockFilterParser(FilterParser):
    def __init__(self):
        super().__init__(
            op_map={"eq": lambda a, b: (a, "eq", b)},
            func_map={"tolower": lambda a: f"lower({a})"}
        )

    def get_column(self, col: str):
        return f"col({col})"


class TestFilterParserBase(unittest.TestCase):
    def setUp(self):
        self.parser = MockFilterParser()

    def test_parse_literals(self):
        # Integer
        self.assertEqual(self.parser.parse_identifier("123"), 123)
        # String
        self.assertEqual(self.parser.parse_identifier("'hello'"), "hello")
        # Boolean
        self.assertEqual(self.parser.parse_identifier("true"), True)
        self.assertEqual(self.parser.parse_identifier("false"), False)
        # Null
        self.assertIsNone(self.parser.parse_identifier("null"))
        # Date
        dt = self.parser.parse_identifier("2023-01-01")
        self.assertIsInstance(dt, datetime)
        # Time
        tm = self.parser.parse_identifier("12:30:00")
        self.assertIsInstance(tm, time)

    def test_parse_column(self):
        self.assertEqual(self.parser.parse_identifier("my_field"), "col(my_field)")

    def test_create_filter_basic(self):
        res = self.parser.create_filter("name eq 'John'")
        self.assertEqual(res, ("col(name)", "eq", "John"))

    def test_function_call(self):
        # Note: function call parsing in FilterParser seems to depend on func_map
        # self.filter_expression = pp.infixNotation(condition, ...)
        # condition = pp.Group(param + operator_ + param)
        # param = function_call | arg
        # function_call = pp.Group(identifier + l_par + pp.Optional(pp.delimitedList(arg)) + r_par)

        res = self.parser.create_filter("toLower(name) eq 'john'")
        self.assertEqual(res, ("lower(col(name))", "eq", "john"))


if __name__ == "__main__":
    unittest.main()
