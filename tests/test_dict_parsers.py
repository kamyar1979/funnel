import unittest

from parser.dict_data import DictFilterParser
from parser.py_ast_dict import DictASTFilterParser
from tests.mock_data import MOCK_ITEMS


class TestDictParsers(unittest.TestCase):
    def test_dict_filter_parser_basic(self):
        data = MOCK_ITEMS[0]
        parser = DictFilterParser(data)

        # Test eq
        self.assertTrue(parser.create_filter("priority eq 1"))
        self.assertFalse(parser.create_filter("priority eq 20"))

        # Test string
        self.assertTrue(parser.create_filter("name eq 'Project Alpha'"))
        self.assertTrue(parser.create_filter("name startswith 'Proj'"))
        self.assertTrue(parser.create_filter("name endswith 'Alpha'"))
        self.assertTrue(parser.create_filter("name like 'ject'"))

    def test_dict_filter_parser_apply(self):
        items = MOCK_ITEMS

        parser = DictFilterParser(MOCK_ITEMS[0])
        # Currently DictFilterParser.apply_filter implementation in dict_data.py is:
        # def apply_filter(self, filter_string: str, items: Sequence):
        #     criteria = self.create_filter(filter_string)
        #     return [data for data in items if criteria]
        # This means it evaluates the filter ONCE against self.data and then returns all or none of items.

        self.assertEqual(len(parser.apply_filter("priority eq 1", items)), len(items))
        self.assertEqual(len(parser.apply_filter("priority eq 2", items)), 0)

    def test_dict_ast_filter_parser(self):
        items = MOCK_ITEMS
        parser = DictASTFilterParser()

        # basic numeric
        res = parser.apply_filter("priority eq 1", items)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["name"], "Project Alpha")
        self.assertEqual(res[1]["name"], "Delta API")

        # string funcs
        res = parser.apply_filter("name startswith 'Proj'", items)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["name"], "Project Alpha")

        # nested
        res = parser.apply_filter("metadata.settings.notifications eq true", items)
        self.assertEqual(len(res), 2)

        # in list
        res = parser.apply_filter("type in ['INTERNAL']", items)
        self.assertEqual(len(res), 2)

        # Verify specific items for 'in'
        names = [item["name"] for item in res]
        self.assertIn("Project Alpha", names)
        self.assertIn("Gamma Service", names)

    def test_dict_ast_filter_parser_complex(self):
        items = MOCK_ITEMS
        parser = DictASTFilterParser()

        # Deeply nested dotted path
        res = parser.apply_filter("metadata.settings.retention_days gt 10", items)
        self.assertEqual(len(res), 2)
        self.assertEqual(set(r["id"] for r in res), {"uuid-1", "uuid-4"})

        # Boolean filter
        res = parser.apply_filter("metadata.settings.notifications eq false", items)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["id"], "uuid-2")

        # Null check
        res = parser.apply_filter("metadata eq null", items)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["id"], "uuid-3")

        # Collection 'has' operator (contains)
        res = parser.apply_filter("tags has 'urgent'", items)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["id"], "uuid-1")

        # List index access
        res = parser.apply_filter("history.0.event eq 'created'", items)
        self.assertEqual(len(res), 2)

        # Multiple conditions with complex nesting
        res = parser.apply_filter("metadata.owner eq 'Alice' AND priority eq 1", items)
        self.assertEqual(len(res), 2)

    def test_dict_ast_filter_parser_edge_cases(self):
        items = [
            {"val": 10.5, "neg": -5, "empty": ""},
            {"val": 20, "neg": -10, "empty": " "},
        ]
        parser = DictASTFilterParser()

        # Float comparison
        res = parser.apply_filter("val gt 15", items)
        self.assertEqual(len(res), 1)

        # Negative numbers
        res = parser.apply_filter("neg lt -7", items)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["neg"], -10)

        # Empty string
        res = parser.apply_filter("empty eq ''", items)
        self.assertEqual(len(res), 1)


if __name__ == "__main__":
    unittest.main()
