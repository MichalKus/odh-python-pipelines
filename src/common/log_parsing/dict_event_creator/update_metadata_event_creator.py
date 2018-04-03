from common.log_parsing.metadata import AbstractEventCreator


class UpdateMetadataEventCreator(AbstractEventCreator):
    """
    Event creator to rename fields
    """

    def __init__(self, metadata):
        """
        Creates event creator
        :param metadata: updated metadata
        """
        AbstractEventCreator.__init__(self, metadata, None, None, None)

    def _create_with_context(self, row, context):
        """
        Converts row to typed values according metadata.
        :param row: input row
        :param context: dictionary with additional data.
        :return: map representing event where key is event field name and value is field value.
        :exception ParsingException if converting goes wrong.
        """

        for field in [self._metadata.get_field_by_index(index) for index in range(self._metadata.get_fields_amount())]:
            row[field.get_output_name()] = row.pop(field.get_name())

        return row

    def contains_fields_to_parse(self, row):
        return self._metadata.get_field_by_index(0).get_name() in row.keys()
