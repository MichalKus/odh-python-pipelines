"""Composite event creator"""


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
        result = {}
        for parser_step in self.__event_creator_list:
            if parser_step.dependent:
                if parser_step.event_creator.contains_fields_to_parse(intermediate_message):
                    result = parser_step.event_creator.create(intermediate_message)
                    intermediate_message.update(result)
            else:
                result = parser_step.event_creator.create(source_message)
                intermediate_message.update(result)
            if result and parser_step.final:
                break
        return intermediate_message


class ParserStep(object):
    """
    One step in CompositeEventCreator, contains event_creator and two flags about dependency from previous step
    and necessity to do next parsing
    """

    def __init__(self, event_creator, dependent, final):
        self.event_creator = event_creator
        self.dependent = dependent
        self.final = final
