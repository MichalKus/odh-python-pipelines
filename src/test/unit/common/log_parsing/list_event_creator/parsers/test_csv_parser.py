import unittest
from common.log_parsing.list_event_creator.parsers.csv_parser import CsvParser


class CsvParseTestCase(unittest.TestCase):
    def test_simple_csv_parse_equals_skipinitialspace(self):
        parser = CsvParser(",", '"', skipinitialspace=True)
        self.assertEquals(["a", "b", "c"], parser.parse('''"a", "b", "c"'''))

    def test_simple_csv_parse_equals(self):
        parser = CsvParser("|", "'")
        self.assertEquals(["a", "b", "c"], parser.parse("""'a'|'b'|'c'"""))

    def test_simple_csv_parse_not_equals(self):
        parser = CsvParser("|", "'")
        self.assertNotEquals(["a", "b", "c"], parser.parse('''"a", "b", "c", "d"'''))
if __name__ == '__main__':
    unittest.main()
