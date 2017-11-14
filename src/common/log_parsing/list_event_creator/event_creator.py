from common.log_parsing.metadata import ParsingException


class EventCreator:
    """
    Creates event for list event parser
    """

    def __init__(self, metadata, parser):
        """
        Creates event creator for list parser
        :param metadata: metadata
        :param parser: list parser
        """
        self._metadata = metadata
        self._parser = parser

    def create(self, row):
        """
        Creates event for given row
        :param row: input row
        :return: dict with result fields
        :raises: ParsingException when values amount isn't equal metadata fields amount
        """
        values = self._convert_row_to_event_values(row)
        if self._metadata.get_fields_amount() == len(values):
            return {
                self._metadata.get_field_by_idex(i).get_output_name(): self._metadata.get_field_by_idex(i).get_value(
                    values[i])
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
