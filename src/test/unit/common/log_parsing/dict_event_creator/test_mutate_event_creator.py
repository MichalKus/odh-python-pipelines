import unittest
from common.log_parsing.dict_event_creator.mutate_event_creator import MutateEventCreator, \
    FieldsMapping
from common.log_parsing.metadata import Metadata, FloatField


class EventCreatorTestCase(unittest.TestCase):

    def test_event_creates_aggregates_values(self):
        event_creator = MutateEventCreator(fields_mappings=[FieldsMapping(["f1", "f2"], "f3", lambda x, y: x + y)])
        self.assertEquals({"f3": 3}, event_creator.create({"f1": 1, "f2": 2}))

    def test_event__creates_aggregated_values_and_removes_intermediate_fields(self):
        event_creator = MutateEventCreator(
            fields_mappings=[FieldsMapping(["f1", "f2"], "f3", lambda x, y: x + y, True)])
        self.assertEquals({"f3": 3}, event_creator.create({"f1": 1, "f2": 2}))

    def test_event_creates_aggregated_string_values(self):
        event_creator = MutateEventCreator(fields_mappings=[FieldsMapping(["f1", "f2"], "f3")])
        self.assertEquals({"f3": "hello world!"},
                          event_creator.create({"f1": "hello", "f2": "world!"}))

    def test_event_creates_aggregated_values_for_multiple_field_groups(self):
        event_creator = MutateEventCreator(
            fields_mappings=[FieldsMapping(["f1", "f2"], "f5", agg_func=lambda x, y: x + y,
                                           remove_intermediate_fields=False),
                             FieldsMapping(["f3", "f4"], "f6", agg_func=lambda x, y: x + y,
                                           remove_intermediate_fields=True)])
        row = {"f1": 1, "f2": 2, "f3": 11, "f4": 12}
        self.assertEquals({"f5": 3, "f6": 23},
                          event_creator.create(row))
        self.assertEquals({"f1": 1, "f2": 2}, row)

    def test_event_creates_with_metadata(self):
        event_creator = MutateEventCreator(Metadata([FloatField("f3", "float_f3")]),
                                           [FieldsMapping(["f1", "f2"], "f3", lambda x, y: x + y)])
        self.assertEquals({"float_f3": 3.0}, event_creator.create({"f1": 1, "f2": 2}))

    def test_with_agg_fields_count_not_equal_to_agg_func_arguments_count_raise_exception(self):
        with self.assertRaises(ValueError):
            MutateEventCreator(None, [FieldsMapping(["f1", "f2", "f3"], "f4", remove_intermediate_fields=True)])


if __name__ == '__main__':
    unittest.main()
