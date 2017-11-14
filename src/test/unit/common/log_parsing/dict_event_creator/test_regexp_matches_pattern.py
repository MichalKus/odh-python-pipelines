import unittest

from common.log_parsing.dict_event_creator.regexp_matches_parser import RegexpMatchesParser
from common.log_parsing.metadata import ParsingException
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser


class RegexpMatchesParserTestCase(unittest.TestCase):
    def test_return_all_named_group(self):
        parser = RegexpMatchesParser("(?P<t1>\w+)")
        self.assertEquals({"t1": "aaa"}, parser.parse("||||aaa|||||aba"))

    def test_return_only_named_group(self):
        parser = RegexpMatchesParser("(?P<term>\D+).(\D+)")
        self.assertEquals({"term": "a"}, parser.parse("123a|b123"))

    def test_throw_exception_if_not_match(self):
        parser = RegexpMatchesParser("(?P<term>\d+)")
        self.assertRaises(ParsingException, parser.parse, "a")


if __name__ == '__main__':
    unittest.main()
