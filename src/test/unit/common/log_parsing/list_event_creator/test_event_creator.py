import unittest
from common.log_parsing.metadata import *
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.splitter_parser import SplitterParser


class EventCreatorTestCase(unittest.TestCase):
    row = {"message": "a|b"}

    def test_event_create_equals_fields_and_values(self):
        metadata = Metadata([
            StringField("f1"),
            StringField("f2")
        ])
        event_creator = EventCreator(metadata, SplitterParser("|"))
        self.assertEquals({"f1": "a", "f2": "b"}, event_creator.create(self.row))

    def test_event_create_less_fields(self):
        metadata = Metadata([
            StringField("f1")
        ])
        event_creator = EventCreator(metadata, SplitterParser("|"))
        self.assertRaises(ParsingException, event_creator.create, self.row)

    def test_event_create_less_values(self):
        metadata = Metadata([
            StringField("f1"),
            StringField("f2"),
            StringField("f3")
        ])
        event_creator = EventCreator(metadata, SplitterParser("|"))
        self.assertRaises(ParsingException, event_creator.create, self.row)


if __name__ == '__main__':
    unittest.main()
