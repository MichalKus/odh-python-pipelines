import unittest
from common.log_parsing.list_event_creator.splitter_parser import SplitterParser


class SplitParseTestCase(unittest.TestCase):
    def test_simple_split_parse_equals(self):
        parser = SplitterParser("|")
        self.assertEquals(["a", "b", "c"], parser.parse("a|b|c"))

    def test_simple_split_parse_not_equals(self):
        parser = SplitterParser("|")
        self.assertNotEquals(["a", "b", "c"], parser.parse("a|b|c|d"))

    def test_simple_split_parse_trim(self):
        parser = SplitterParser("|", is_trim=True)
        self.assertEquals(["a", "b", "c"], parser.parse("a   |    b    |     c"))


if __name__ == '__main__':
    unittest.main()
