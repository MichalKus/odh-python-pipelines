class EventCreator:
    """
    Creates event for dict parser
    """

    def __init__(self, metadata, parser, field_to_parse="message"):
        """
        Creates instance for dict parser
        :param metadata: metadata
        :param parser: dict parser
        :param field_to_parse: field that uses as source for creating event
        """
        self.__metadata = metadata
        self.__parser = parser
        self.__field_to_parse = field_to_parse

    def create(self, row):
        """
        Creates event for given row
        :param row: input row
        :return: dict with result fields
        """
        return {
            self.__metadata.get_field_by_name(field).get_output_name():
                self.__metadata.get_field_by_name(field).get_value(value)
            for field, value in self.__parser.parse(row[self.__field_to_parse]).items()
        }

    def get_field_to_parse(self):
        return self.__field_to_parse


class CompositeEventCreator:
    def __init__(self):
        self.__event_creator_list = list()

    def add_source_parser(self, event_creator):
        """
        Event creator added using this method uses source event that event produced on previous step.
        For example: original message is {'message':'event is 115566"}, intermediate result that we got from
        previous parser is {'message':'event is 115566', 'event_id':'event is 115566'}. For event creator added using
        this method is used original message as input result.

        :param event_creator: class that are responsible for parsing
        """
        self.__event_creator_list.append((False, event_creator))
        return self

    def add_intermediate_result_parser(self, event_creator):
        """
        Event creator added using this method uses event that produced by previous step rather than source event.
        For example: original message is {'message':'event is 115566"}, intermediate result that we got from
        previous parser is {'message':'event is 115566', 'event_id':'event is 115566'}. For event creator added using
        this method is used intermediate result as input.

        :param event_creator: class that are responsible for parsing
        """
        self.__event_creator_list.append((True, event_creator))
        return self

    def create(self, source_message):
        intermediate_message = {}

        for dependent, event_creator in self.__event_creator_list:

            if dependent:
                if event_creator.get_field_to_parse() in intermediate_message:
                    intermediate_message.update(event_creator.create(intermediate_message))
            else:
                intermediate_message.update(event_creator.create(source_message))

        return intermediate_message
