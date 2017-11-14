import unittest

from common.log_parsing.metadata import ParsingException
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser


class RegexpParserTestCase(unittest.TestCase):
    def test_return_all_named_group(self):
        parser = RegexpParser("(?P<t1>\w+).(?P<t2>\w+).(?P<t3>\w+)")
        self.assertEquals({"t1": "a", "t2": "b", "t3": "c"}, parser.parse("a|b|c"))

    def test_return_only_named_group(self):
        parser = RegexpParser("(?P<term>\w+).(\w+)")
        self.assertEquals({"term": "a"}, parser.parse("a|b"))

    def test_throw_exception_if_not_match(self):
        parser = RegexpParser("(?P<term>\d+)")
        self.assertRaises(ParsingException, parser.parse, "a")

    def test_return_none_if_does_not_match(self):
        parser = RegexpParser("(?P<term>\d+)", return_empty_dict=True)
        self.assertEquals({}, parser.parse("a"))


if __name__ == '__main__':
    unittest.main()
