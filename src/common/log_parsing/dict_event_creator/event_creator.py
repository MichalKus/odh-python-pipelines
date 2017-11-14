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


class CompositeEventCreator:
    def __init__(self):
        self.__event_creator_list = list()

    def add(self, event_creator):
        self.__event_creator_list.append(event_creator)
        return self

    def create(self, row):
        dict = {}

        for event_creator in self.__event_creator_list:
            dict.update(event_creator.create(row))

        return dict
