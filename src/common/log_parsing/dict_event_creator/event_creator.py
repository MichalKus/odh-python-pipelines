"""Module with all necessary tools for creating new events during parsing log messages"""
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

    def parse_if_field_exist(self, row):
        if self.__field_to_parse in row:
            return self.create(row)
        else:
            return {}


class ParserStep(object):
    """
    One step in CompositeEventCreator, contains event_creator and two flags about dependency from previous step
    and necessity to do next parsing
    """

    def __init__(self, event_creator, dependent, final):
        self.event_creator = event_creator
        self.dependent = dependent
        self.final = final


class CompositeEventCreator(object):
    """Extension for event creator with several steps of parsing messages and their sub messages"""

    def __init__(self):
        self.__event_creator_list = list()

    def add_source_parser(self, event_creator, final=False):
        """
        Event creator added using this method uses source event that event produced on previous step.
        For example: original message is {'message':'event is 115566"}, intermediate result that we got from
        previous parser is {'message':'event is 115566', 'event_id':'event is 115566'}. For event creator added using
        this method is used original message as input result.

        :param final: Flag shows if we should continue process next event creators or not
        :param event_creator: class that are responsible for parsing
        """
        self.__event_creator_list.append(ParserStep(event_creator, dependent=False, final=final))
        return self

    def add_intermediate_result_parser(self, event_creator, final=False):
        """
        Event creator added using this method uses event that produced by previous step rather than source event.
        For example: original message is {'message':'event is 115566"}, intermediate result that we got from
        previous parser is {'message':'event is 115566', 'event_id':'event is 115566'}. For event creator added using
        this method is used intermediate result as input.

        :param final: Flag shows if we should continue process next event creators or not
        :param event_creator: class that are responsible for parsing
        """
        self.__event_creator_list.append(ParserStep(event_creator, dependent=True, final=final))
        return self

    def create(self, source_message):
        intermediate_message = {}
        for parser_step in self.__event_creator_list:
            if parser_step.dependent:
                result = parser_step.event_creator.parse_if_field_exist(intermediate_message)
                intermediate_message.update(result)
            else:
                result = parser_step.event_creator.create(source_message)
                intermediate_message.update(result)
            if result and parser_step.final:
                break
        return intermediate_message
