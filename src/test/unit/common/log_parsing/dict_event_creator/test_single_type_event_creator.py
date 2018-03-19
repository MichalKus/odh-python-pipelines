import unittest

from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.log_parsing.dict_event_creator.key_value_parser import KeyValueParser
from common.log_parsing.metadata import *


class SingleTypeEventCreatorTestCase(unittest.TestCase):
    row = {"message": " KeyOne = 1 , KeyTwo = 2"}

    def test_event_creates_fields_and_values(self):
        event_creator = SingleTypeEventCreator(IntField(None), KeyValueParser(",", "="))
        self.assertEquals({"key_one": 1, "key_two": 2}, event_creator.create({"message": " KeyOne = 1 , KeyTwo = 2"}))

    def test_event_removes_empty_values(self):
        event_creator = SingleTypeEventCreator(IntField(None), KeyValueParser(",", "="))
        self.assertEquals({"key_two": 2}, event_creator.create({"message": " KeyOne =  , KeyTwo = 2"}))


if __name__ == '__main__':
    unittest.main()
