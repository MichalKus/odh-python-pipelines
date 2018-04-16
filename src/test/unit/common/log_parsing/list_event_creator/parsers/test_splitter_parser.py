import unittest
from common.log_parsing.list_event_creator.parsers.splitter_parser import SplitterParser


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

    def test_split_parse_with_max_split(self):
        parser = SplitterParser("|", is_trim=True, max_split=2)
        self.assertEquals(["a", "b   | c   | a"], parser.parse("a   |   b   | c   | a"))

    def test_split_parse_with_max_split_bigger_than_split_result_size(self):
        parser = SplitterParser("|", is_trim=True, max_split=100)
        self.assertEquals(["a", "b", "c", "a"], parser.parse("a   |   b   | c   | a"))

    def test_split_parse_with_invalid_max_split(self):
        with self.assertRaises(ValueError):
            SplitterParser("|", max_split=0)


if __name__ == '__main__':
    unittest.main()
