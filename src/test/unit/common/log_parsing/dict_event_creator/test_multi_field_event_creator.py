import unittest

from common.log_parsing.dict_event_creator.multi_fields_event_creator import MultiFieldsEventCreator
from common.log_parsing.metadata import *


class MultiFieldEventCreatorTestCase(unittest.TestCase):
    value_type = StringField("output_field_name")
    row = {"field1": "abc", "field2": "xyz", "field3": "****"}

    def test_event_create_field_with_matched_value(self):
        event_creator = MultiFieldsEventCreator(self.value_type,
                                                ["field1", "field2"], [(["a", "*"], "value1"), (["a", "z"], "value2")])
        self.assertEquals({"output_field_name": "value2"}, event_creator.create(self.row))

    def test_event_create_field_with_matched_value_in_revert_order(self):
        event_creator = MultiFieldsEventCreator(self.value_type,
                                                ["field1", "field2"], [(["a", "z"], "value2"), (["a", "*"], "value1")])
        self.assertEquals({"output_field_name": "value2"}, event_creator.create(self.row))

    def test_event_create_no_field(self):
        event_creator = MultiFieldsEventCreator(self.value_type,
                                                ["field1", "field2"], [(["1", "2"], "value1"), (["3", "4"], "value2")])
        self.assertEquals({}, event_creator.create(self.row))

    def test_with_wrong_fields_count_and_match_fields_count_raise_exception(self):
        with self.assertRaises(ValueError):
            MultiFieldsEventCreator(self.value_type,
                                    ["field1"], [(["/", "*"], "value1"), (["a"], "value2")])

    def test_event_create_field_with_full_matched_value(self):
        event_creator = MultiFieldsEventCreator(self.value_type,
                                                ["field1", "field2"],
                                                [(["abc", "xyz"], "value2")], True)
        self.assertEquals({"output_field_name": "value2"}, event_creator.create(self.row))

    def test_event_create_no_field_with_not_full_matched_value(self):
        event_creator = MultiFieldsEventCreator(self.value_type,
                                                ["field1", "field2"],
                                                [(["abc", "xy"], "value2")], True)
        self.assertEquals({}, event_creator.create(self.row))


if __name__ == '__main__':
    unittest.main()
