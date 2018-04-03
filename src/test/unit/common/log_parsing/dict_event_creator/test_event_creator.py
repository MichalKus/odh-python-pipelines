import unittest

from common.log_parsing.metadata import *
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser


class EventCreatorTestCase(unittest.TestCase):
    metadata = Metadata([
        StringField("term1", "term1_result"),
        StringField("term2"),
    ])
    row = {"message": "a|b"}

    def test_event_create_equals_fields_and_values(self):
        event_creator = EventCreator(self.metadata, RegexpParser("(?P<term1>\w+).(?P<term2>\w+)"))
        self.assertEquals({"term1_result": "a", "term2": "b"}, event_creator.create(self.row))


if __name__ == '__main__':
    unittest.main()
