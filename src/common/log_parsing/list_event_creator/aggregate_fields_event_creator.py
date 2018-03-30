from inspect import getargspec

from common.log_parsing.list_event_creator.event_creator import EventCreator


class AggregateFieldsEventCreator(EventCreator):
    """
    Extended EventCreator with checking for equals of started and finished scripts
    """

    def __init__(self, metadata, parser, fields_to_aggregate=None, result_field=None, remove_intermediate_fields=False,
                 agg_func=lambda x, y: x + y,
                 matcher=None,
                 field_to_parse="message", timezone_field="tz"):
        """
        Creates extended event creator
        :param metadata: metadata
        :param parser: dict parser
        :param matcher: matcher object to check the input line to perform the parsing step only if the line is matched
        :param field_to_parse: field that uses as source for creating event
        :param timezone_field: field name with information about timezone
        :param fields_to_aggregate: fields that should be aggregated after initial parsing
        :param result_field: field that will contain result of aggregation
        :param remove_intermediate_fields: boolean
        """
        if not callable(agg_func) or len(getargspec(agg_func).args) != len(fields_to_aggregate):
            raise ValueError(
                "Aggregate function must take same arguments count as "
                "fields_to_aggregate count and produce single argument!")
        self._metadata = metadata
        self._parser = parser
        self.__fields_to_aggregate = fields_to_aggregate
        self.__result_field = result_field
        self.__remove_intermediate_fields = remove_intermediate_fields
        self.__agg_func = agg_func
        EventCreator.__init__(self, metadata, parser, matcher, field_to_parse, timezone_field)

    def create(self, row):
        """
        Parse row with Parser and then check for equals of started and finished script names
        :param row: Row from kafka topic
        :return: list of all fields
        """
        values = super(AggregateFieldsEventCreator, self).create(row)
        if self.__fields_to_aggregate and self.__result_field:
            values_to_agg = map(lambda x: values[x], self.__fields_to_aggregate)
            result_value = reduce(self.__agg_func, values_to_agg)
            if self.__remove_intermediate_fields:
                for field in self.__fields_to_aggregate:
                    del values[field]
            values.update({self.__result_field: result_value})
        return values
