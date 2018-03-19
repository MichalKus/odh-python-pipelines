import unittest

from common.log_parsing.metadata import ParsingException
from common.log_parsing.dict_event_creator.key_value_parser import KeyValueParser


class KeyValueParserTestCase(unittest.TestCase):
    parser = KeyValueParser(",", "=")

    def test_parse_non_empty_fields(self):
        self.assertEquals({"t1": "a", "t2": "b", "t3": "c"}, self.parser.parse("t1 = a , t2 = b , t3 = c "))

    def test_parse_with_empty_field(self):
        self.assertEquals({"t1": "a", "t2": "b", "t3": "c"}, self.parser.parse("t1 = a , t2 = b , t3 = c , t4 = "))

    def test_parse_with_several_empty_fields(self):
        self.assertEquals({"t2": "b"}, self.parser.parse("t1 = , t2 = b , t3 =  , t4 = "))

    def test_parse_with_all_empty_fields(self):
        self.assertEquals({}, self.parser.parse("t1 =  , t2 =  "))

    def test_parse_with_no_spaces(self):
        self.assertEquals({"t2": "1"}, self.parser.parse("t1=,t2=1"))

    def test_throw_exception_if_nothing_to_parse(self):
        with self.assertRaises(ParsingException):
            self.parser.parse("invalid format")

    def test_throw_exception_if_invalid_format(self):
        with self.assertRaises(ParsingException):
            self.parser.parse("invalid1 , invalid2")

    def test_throw_exception_if_any_key_is_empty(self):
        with self.assertRaises(ParsingException):
            self.parser.parse(" = invalid1 , key = invalid2")

    def test_parse_and_transform_keys_to_underscore_format(self):
        self.assertEquals({"metric_name": "b"}, self.parser.parse("t1 = , MetricName = b , t3 =  , t4 = "))


if __name__ == '__main__':
    unittest.main()
