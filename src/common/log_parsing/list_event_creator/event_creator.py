from common.log_parsing.metadata import ParsingException, AbstractEventCreator

class EventCreator(AbstractEventCreator):
    """
    Creates event for list event parser
    """

    def __init__(self, metadata, parser, timezone_field="tz"):
        """
        Creates event creator for list parser
        :param metadata: metadata
        :param parser: list parser
        """
        AbstractEventCreator.__init__(self, metadata, parser, timezone_field)

    def _create_with_context(self, row, context):
        values = self._convert_row_to_event_values(row)
        if self._metadata.get_fields_amount() == len(values):
            return {
                self._metadata.get_field_by_idex(i).get_output_name(): self._metadata.get_field_by_idex(i).get_value(
                    values[i], context)
                for i in range(len(values))
            }

        else:
            raise ParsingException("Fields amount not equal values amount")

    def _convert_row_to_event_values(self, row):
        """
        Converts given row to event values
        :param row: input row
        :return: list of values
        """
        return self._parser.parse(row["message"])

