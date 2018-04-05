"""Module with EventCreator class"""
from common.log_parsing.metadata import AbstractEventCreator


class EventCreator(AbstractEventCreator):
    """
    Creates event for dict parser
    """

    def __init__(self, metadata, parser, matcher=None, field_to_parse="message", timezone_field="tz"):
        """
        Creates instance for dict parser
        :param metadata: metadata
        :param parser: dict parser
        :param matcher: matcher object to check the input line to perform the parsing step only if the line is matched
        :param field_to_parse: field that uses as source for creating event
        :param timezone_field: field name with information about timezone
        """
        AbstractEventCreator.__init__(self, metadata, parser, matcher, timezone_field)
        self.__field_to_parse = field_to_parse

    def _create_with_context(self, row, context):
        """
        Converts row to typed values according metadata.
        :param row: input row
        :param context: dictionary with additional data.
        :return: map representing event where key is event field name and value is field value.
        :exception ParsingException if converting goes wrong.
        """
        return {
            self._metadata.get_field_by_name(field).get_output_name():
                self._metadata.get_field_by_name(field).get_value(value, context)
            for field, value in self._parser.parse(row[self.__field_to_parse]).items()
        } if self._matcher is None or self._matcher.match(row[self.__field_to_parse]) else {}

    def contains_fields_to_parse(self, row):
        return self.__field_to_parse in row
