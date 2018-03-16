"""
Creator for traxis_frontend:
- KeyValueEventCreator
"""
from common.log_parsing.dict_event_creator.event_creator import EventCreator


class KeyValueEventCreator(EventCreator):

    def _create_with_context(self, row, context):
        """
       Converts row to typed values according metadata.
       :param row: input row
       :param context: dictionary with additional data.
       :return: map representing event where key is event field name and value is field value.
       :exception ParsingException if converting goes wrong.
       """
        parsed = self._parser.parse(row[self.get_field_to_parse()])
        return {
            field:
                self._metadata.get_field_by_name("any_field").get_value(value, context)
            for field, value in parsed.items()
        } if self._matcher is None or self._matcher.match(row[self.__field_to_parse]) else {}
