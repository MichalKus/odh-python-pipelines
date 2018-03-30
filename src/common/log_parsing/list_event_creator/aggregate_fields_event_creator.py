"""
    Event creator that can aggregate fields after parsing
"""
from inspect import getargspec

from common.log_parsing.list_event_creator.event_creator import EventCreator


class AggregateFieldsEventCreator(EventCreator):
    """
    Event creator that can aggregate fields after initial parsing
    """

    def __init__(self, metadata, parser, fields_mappings=None, agg_func=lambda x, y: x + y,
                 matcher=None, field_to_parse="message", timezone_field="tz"):
        """
        Creates extended event creator
        :param metadata: metadata
        :param parser: parser
        :param matcher: matcher object to check the input line to perform the parsing step only if the line is matched
        :param field_to_parse: field that uses as source for creating event
        :param timezone_field: field name with information about timezone
        :param fields_mappings: list of FieldsMappings
        """

        for fields_mapping in fields_mappings:
            if not callable(agg_func) or len(getargspec(agg_func).args) != len(
                fields_mapping.get_fields_to_aggregate()):
                raise ValueError(
                    "Aggregate function must take same arguments count as "
                    "fields_to_aggregate count and produce single argument!")

        self._metadata = metadata
        self._parser = parser
        self.__fields_mappings = fields_mappings
        self.__agg_func = agg_func
        EventCreator.__init__(self, metadata, parser, matcher, field_to_parse, timezone_field)

    def create(self, row):
        """
        Parse row with Parser and then check for equals of started and finished script names
        :param row: Row from kafka topic
        :return: list of all fields
        """

        values = super(AggregateFieldsEventCreator, self).create(row)
        for fields_mapping in self.__fields_mappings:
            values_to_agg = map(lambda x: values[x], fields_mapping.get_fields_to_aggregate())
            result_value = reduce(self.__agg_func, values_to_agg)
            if fields_mapping.get_remove_intermediate_fields():
                for field in fields_mapping.get_fields_to_aggregate():
                    del values[field]
            values.update({fields_mapping.get_result_field(): result_value})

        return values


class FieldsMapping(object):
    """
    Case class that contain list of fields to aggregate,
    result field and boolean flag that indicates removing of intermediate fields
    """
    def __init__(self, fields_to_aggregate, result_field, remove_intermediate_fields=False):
        if (not fields_to_aggregate and not isinstance(fields_to_aggregate, list)) \
            or (not result_field and isinstance(result_field, str)):
            raise ValueError("Expected not None arguments")
        self.__fields_to_aggregate = fields_to_aggregate
        self.__result_field = result_field
        self.__remove_intermediate_fields = remove_intermediate_fields

    def get_fields_to_aggregate(self):
        return self.__fields_to_aggregate

    def get_result_field(self):
        return self.__result_field

    def get_remove_intermediate_fields(self):
        return self.__remove_intermediate_fields
