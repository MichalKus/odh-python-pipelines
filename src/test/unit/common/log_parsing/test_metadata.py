import unittest

from dateutil.tz import tzoffset

from common.log_parsing.metadata import *
from datetime import datetime
from common.log_parsing.timezone_metadata import *

class EventCreatorTestCase(unittest.TestCase):
    metadata = Metadata([
        StringField("string"),
        TimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f"),
        ConfigurableTimestampField("configurable_time", "%Y-%m-%d %H:%M:%S", "UTC", priorities="idc",
                                   include_timezone=True),
        IntField("int"),
        FloatField("float")
    ])

    def test_string_field(self):
        self.assertEquals("value", self.metadata.get_field_by_name("string").get_value("value"))

    def test_timestamp_field_success(self):
        self.assertEquals(datetime(2017, 9, 28, 13, 39, 11, 238000),
                          self.metadata.get_field_by_name("timestamp").get_value("2017-09-28 13:39:11,238"))

    def test_timestamp_field_wrong_format(self):
        self.assertRaises(ParsingException, self.metadata.get_field_by_name("timestamp").get_value, "2017-09-28")

    def test_int_field_success(self):
        self.assertEquals(123, self.metadata.get_field_by_name("int").get_value("123"))

    def test_int_field_parse_error(self):
        self.assertRaises(ParsingException, self.metadata.get_field_by_name("int").get_value, "abc")

    def test_float_field_success(self):
        self.assertEquals(1.23, self.metadata.get_field_by_name("float").get_value("1.23"))

    def test_float_field_parse_error(self):
        self.assertRaises(ParsingException, self.metadata.get_field_by_name("float").get_value, "abc")

    def test_ConfigurableTimestampField(self):
        self.assertEquals(datetime(2017, 9, 28, 11, 39, 11).replace(tzinfo=pytz.utc),
                          self.metadata.get_field_by_name("configurable_time").get_value("2017-09-28 13:39:11 +0200"))


if __name__ == '__main__':
    unittest.main()
