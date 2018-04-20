import unittest
from common.log_parsing.metadata import *
from common.log_parsing.list_event_creator.multiple_event_creator import MultipleEventCreator
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.parsers.splitter_parser import SplitterParser


class EventCreatorTestCase(unittest.TestCase):
    event_creators = MultipleEventCreator([
        EventCreator(
            Metadata([
                StringField("f1")
            ]),
            SplitterParser("|")
        ),
        EventCreator(
            Metadata([
                StringField("f1"),
                StringField("f2")
            ]),
            SplitterParser("|")
        )
    ])

    def test_event_create_one_field(self):
        self.assertEquals({"f1": "a"}, self.event_creators.create({"message": "a"}))

    def test_event_create_two_fields(self):
        self.assertEquals({"f1": "a", "f2": "b"}, self.event_creators.create({"message": "a|b"}))

    def test_event_create_exception(self):
        self.assertRaises(ParsingException, self.event_creators.create, {"message": "a|b|c"})


if __name__ == '__main__':
    unittest.main()
