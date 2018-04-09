import unittest

from common.log_parsing.metadata import ParsingException
from common.log_parsing.list_event_creator.parsers.regexp_parser import RegexpParser


class RegexpParserTestCase(unittest.TestCase):
    def test_simple_pattern_without_full_match(self):
        parser = RegexpParser("(\w+)", match=False)
        self.assertEquals(["a", "b", "c"], parser.parse("a|b|c"))

    def test_simple_pattern_with_full_match(self):
        parser = RegexpParser("(\w+).(\w+).(\w+)")
        self.assertEquals(["a", "b", "c"], parser.parse("a|b|c"))

    def test_simple_pattern_not_matched_without_full_match(self):
        parser = RegexpParser("(d)", match=False)
        self.assertEquals([], parser.parse("a|b|c"))

    def test_simple_pattern_not_matched_with_full_match(self):
        parser = RegexpParser("(d)")
        self.assertRaises(ParsingException, parser.parse, "a|b|c")


if __name__ == '__main__':
    unittest.main()
