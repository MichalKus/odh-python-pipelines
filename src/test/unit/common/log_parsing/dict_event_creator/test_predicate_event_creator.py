import unittest

from common.log_parsing.dict_event_creator.mutate_event_creator import MutateEventCreator, FieldsMapping
from common.log_parsing.dict_event_creator.predicate_event_creator import PredicateEventCreator
from common.log_parsing.metadata import *


class MultiFieldEventCreatorTestCase(unittest.TestCase):
    value_type = StringField("output_field")
    row = {"field1": "abc", "field2": "xyz", "field3": "****"}

    event_creator = MutateEventCreator(None, [FieldsMapping(["field1"], "output_field")], lambda x: "value2")

    def test_event_create_field_with_matched_value(self):
        predicate_event_creator = PredicateEventCreator(["field1", "field2"], [(["a", "*"], None),
                                                                               (["a", "z"], self.event_creator)])
        self.assertEquals({"output_field": "value2"}, predicate_event_creator.create(self.row))

    def test_event_create_field_with_matched_value_in_revert_order(self):
        predicate_event_creator = PredicateEventCreator(["field1", "field2"], [(["a", "z"], self.event_creator),
                                                                               (["a", "*"], None)])
        self.assertEquals({"output_field": "value2"}, predicate_event_creator.create(self.row))

    def test_event_create_no_field(self):
        predicate_event_creator = PredicateEventCreator(["field1", "field2"], [(["1", "2"], "value1"),
                                                                               (["3", "4"], "value2")])
        self.assertEquals({}, predicate_event_creator.create(self.row))

    def test_with_wrong_fields_count_and_match_fields_count_raise_exception(self):
        with self.assertRaises(ValueError):
            PredicateEventCreator(self.value_type,
                                  ["field1"], [(["/", "*"], "value1"), (["a"], "value2")])

    def test_event_create_field_with_full_matched_value(self):
        predicate_event_creator = PredicateEventCreator(["field1", "field2"], [(["abc", "xyz"], self.event_creator)],
                                                        True)
        self.assertEquals({"output_field": "value2"}, predicate_event_creator.create(self.row))

    def test_event_create_no_field_with_not_full_matched_value(self):
        predicate_event_creator = PredicateEventCreator(["field1", "field2"], [(["abc", "xy"], self.event_creator)],
                                                        True)
        self.assertEquals({}, predicate_event_creator.create(self.row))

    def test_event_creates_with_matched_dictionary(self):
        predicate_event_creator = PredicateEventCreator(["field1", "field2"], [(["abc", "xyz"],
                                                                                {"output_field": "value2"})])
        self.assertEquals({"output_field": "value2"}, predicate_event_creator.create(self.row))


if __name__ == '__main__':
    unittest.main()
