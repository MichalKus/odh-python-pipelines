from common.log_parsing.metadata import ParsingException, AbstractEventCreator


class EventCreator(AbstractEventCreator):
    """
    Creates event for list event parser
    """

    def __init__(self, metadata, parser, matcher=None, field_to_parse="message", timezone_field="tz"):
        """
        Creates event creator for list parser
        :param metadata: metadata
        :param parser: list parser
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
        if self._matcher is None or self._matcher.match(row[self.__field_to_parse]):
            values = self._convert_row_to_event_values(row)
            if self._metadata.get_fields_amount() == len(values):
                return {
                    self._metadata.get_field_by_idex(i).get_output_name():
                        self._metadata.get_field_by_idex(i).get_value(values[i], context)
                    for i in range(len(values))
                }
            elif len(values) == 0:
                return {}
            else:
                raise ParsingException("Fields amount not equal values amount")
        else:
            return {}

    def _convert_row_to_event_values(self, row):
        """
        Converts given row to a number of string values
        :param row: input row
        :return: list of values
        """
        return self._parser.parse(row[self.__field_to_parse])

    def get_field_to_parse(self):
        return self.__field_to_parse
