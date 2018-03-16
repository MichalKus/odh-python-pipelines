import unittest

from applications.log_parsing.traxis_frontend.key_value_event_creator import KeyValueEventCreator
from common.log_parsing.dict_event_creator.key_value_parser import KeyValueParser
from common.log_parsing.metadata import *

class KeyValueEventCreatorTestCase(unittest.TestCase):
    metadata = Metadata([
        IntField("any_field"),
    ])
    row = {"message": " KeyOne = 1 , KeyTwo = 2"}

    def test_event_create_equals_fields_and_values(self):
        event_creator = KeyValueEventCreator(self.metadata, KeyValueParser())
        self.assertEquals({"key_one": 1, "key_two": 2}, event_creator.create(self.row))


if __name__ == '__main__':
    unittest.main()
