import unittest

from common.log_parsing.matchers.matcher import SubstringMatcher
from common.log_parsing.metadata import *
from common.log_parsing.dict_event_creator.event_creator import EventCreator, CompositeEventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser


class CompositeEventCreatorTestCase(unittest.TestCase):
    metadata = Metadata([
        StringField("term1"),
        StringField("term2"),
    ])
    row1 = {"message": "a|first_case URL = 123"}
    row2 = {"message": "a|second_case Method: 'ABC'"}
    row3 = {"message": "a|third_case Duration     '5' ms"}

    main_event_creator = EventCreator(metadata, RegexpParser("(?P<term1>\w+).(?P<term2>.*)"))

    def test_composite_event_create_equals_fields_and_values(self):
        url_dependent_metadata = Metadata([
            StringField("case"),
            StringField("url"),
        ])
        url_dependent_event_creator = EventCreator(url_dependent_metadata,
                                                   RegexpParser("(?P<case>\w+) URL = (?P<url>.*)", return_empty_dict=True),
                                                   field_to_parse="term2")

        method_dependent_metadata = Metadata([
            StringField("case"),
            StringField("method"),
        ])
        method_dependent_event_creator = EventCreator(method_dependent_metadata,
                                                      RegexpParser("(?P<case>\w+) Method: '(?P<method>\w+)'", return_empty_dict=True),
                                                      field_to_parse="term2")

        duration_dependent_metadata = Metadata([
            StringField("case"),
            StringField("duration"),
        ])
        duration_dependent_event_creator = EventCreator(duration_dependent_metadata,
                                                        RegexpParser("(?P<case>\w+) Duration\s*'(?P<duration>.*)'", return_empty_dict=True),
                                                        field_to_parse="term2")

        event_creator = CompositeEventCreator() \
            .add_source_parser(self.main_event_creator) \
            .add_intermediate_result_parser(url_dependent_event_creator, final=True) \
            .add_intermediate_result_parser(method_dependent_event_creator, final=True) \
            .add_intermediate_result_parser(duration_dependent_event_creator, final=True)

        self.assertEquals({"term1": "a", "term2": "first_case URL = 123", "case": "first_case", "url": "123"},
                          event_creator.create(self.row1))
        self.assertEquals({"term1": "a", "term2": "second_case Method: 'ABC'", "case": "second_case", "method": "ABC"},
                          event_creator.create(self.row2))
        self.assertEquals({"term1": "a", "term2": "third_case Duration     '5' ms", "case": "third_case", "duration": "5"},
            event_creator.create(self.row3))


    def test_composite_event_create_substring_matching(self):
        url_dependent_metadata = Metadata([
            StringField("case"),
            StringField("url"),
        ])
        url_dependent_event_creator = EventCreator(url_dependent_metadata,
                                                   RegexpParser("(?P<case>\w+) URL = (?P<url>.*)", return_empty_dict=True),
                                                   matcher=SubstringMatcher("first_case"),
                                                   field_to_parse="term2")

        method_dependent_metadata = Metadata([
            StringField("case"),
            StringField("method"),
        ])
        method_dependent_event_creator = EventCreator(method_dependent_metadata,
                                                      RegexpParser("(?P<case>\w+) Method: '(?P<method>\w+)'"),
                                                      matcher=SubstringMatcher("first_case"),
                                                      field_to_parse="term2")

        event_creator = CompositeEventCreator() \
            .add_source_parser(self.main_event_creator) \
            .add_intermediate_result_parser(url_dependent_event_creator, final=True) \
            .add_intermediate_result_parser(method_dependent_event_creator, final=True) \

        self.assertEquals({"term1": "a", "term2": "first_case URL = 123", "case": "first_case", "url": "123"},
                          event_creator.create(self.row1))
        self.assertEquals({"term1": "a", "term2": "second_case Method: 'ABC'"}, event_creator.create(self.row2))


if __name__ == '__main__':
    unittest.main()
