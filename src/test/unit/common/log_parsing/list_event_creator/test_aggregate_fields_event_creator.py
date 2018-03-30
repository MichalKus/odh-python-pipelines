import unittest
from common.log_parsing.metadata import *
from common.log_parsing.list_event_creator.aggregate_fields_event_creator import AggregateFieldsEventCreator, \
    FieldsMapping
from common.log_parsing.list_event_creator.splitter_parser import SplitterParser


class EventCreatorTestCase(unittest.TestCase):
    row = {"message": "1|2"}

    def test_event_creates_aggregates_values(self):
        metadata = Metadata([
            IntField("f1"),
            IntField("f2")
        ])
        event_creator = AggregateFieldsEventCreator(metadata, SplitterParser("|"),
                                                    [FieldsMapping(["f1", "f2"], "f3")])
        self.assertEquals({"f1": 1, "f2": 2, "f3": 3}, event_creator.create(self.row))

    def test_event__creates_aggregated_values_and_removes_intermediate_fields(self):
        metadata = Metadata([
            IntField("f1"),
            IntField("f2")
        ])
        event_creator = AggregateFieldsEventCreator(metadata, SplitterParser("|"),
                                                    [FieldsMapping(["f1", "f2"], "f3", True)])
        self.assertEquals({"f3": 3}, event_creator.create(self.row))

    def test_event_creates_aggregated_string_values(self):
        metadata = Metadata([
            StringField("f1"),
            StringField("f2")
        ])
        event_creator = AggregateFieldsEventCreator(metadata, SplitterParser("|", is_trim=True),
                                                    [FieldsMapping(["f1", "f2"], "f3")],
                                                    agg_func=lambda x, y: x + " " + y)
        self.assertEquals({"f1": "hello", "f2": "world!", "f3": "hello world!"},
                          event_creator.create({"message": "hello | world!"}))

    def test_event_creates_aggregated_values_for_multiple_field_groups(self):
        metadata = Metadata([
            IntField("f1"),
            IntField("f2"),
            IntField("f3"),
            IntField("f4")
        ])
        event_creator = AggregateFieldsEventCreator(metadata, SplitterParser("|", is_trim=True),
                                                    [FieldsMapping(["f1", "f2"], "f5",
                                                                   remove_intermediate_fields=False),
                                                     FieldsMapping(["f3", "f4"], "f6",
                                                                   remove_intermediate_fields=True)])
        self.assertEquals({"f1": 1, "f2": 2, "f5": 3, "f6": 23},
                          event_creator.create({"message": "1 | 2 | 11 | 12"}))

    def test_with_agg_fields_count_not_equal_to_agg_func_arguments_count_raise_exception(self):
        metadata = Metadata([
            StringField("f1"),
            StringField("f2"),
            StringField("f3")
        ])
        with self.assertRaises(ValueError):
            AggregateFieldsEventCreator(metadata, SplitterParser("|", is_trim=True),
                                        [FieldsMapping(["f1", "f2", "f3"], "f4", True)],
                                        agg_func=lambda x, y: x + " " + y)


if __name__ == '__main__':
    unittest.main()
