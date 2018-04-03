import unittest

from common.log_parsing.dict_event_creator.json_parser import JsonParser
from common.log_parsing.metadata import ParsingException


class JsonParserTestCase(unittest.TestCase):
    parser = JsonParser()

    def test_parse_non_empty_fields(self):
        self.assertEquals({"t1": "a", "t2": "b", "t3": "c"}, self.parser.parse(
            "{\"t1\":\"a\",\"t2\":\"b\",\"t3\":\"c\" }"))

    def test_parse_with_empty_fields(self):
        self.assertEquals({"t1": "", "t2": "b", "t3": "c"}, self.parser.parse(
            "{\"t1\":\"\",\"t2\":\"b\",\"t3\":\"c\" }"))

    def test_parse_nested_json_string(self):
        self.assertEquals({"t1": {"x1": "d", "x2": ""}, "t2": "b", "t3": "c"}, self.parser.parse(
            "{"
            "\"t1\":{"
            "\"x1\":\"d\","
            "\"x2\":\"\""
            "},"
            "\"t2\":\"b\","
            "\"t3\":\"c\""
            "}"))

    def test_parse_text_with_invalid_format(self):
        with self.assertRaises(ParsingException):
            self.parser.parse("invalid1 , invalid2")


if __name__ == '__main__':
    unittest.main()
