"""Event creator that tries to use several list event creators"""
from common.log_parsing.metadata import ParsingException


class MultipleEventCreator(object):
    """
    Creates event using several list event creators
    """

    def __init__(self, event_creators):
        """
        Creates event using several list event creators
        :param event_creators: list of event creators
        """
        self._event_creators = event_creators

    def create(self, row):
        """
        Creates event for given row
        :param row: input row
        :return: dict with result fields
        :raises: ParsingException when no one event creator parsed row
        """
        for event_creator in self._event_creators:
            try:
                return event_creator.create(row)
            except ParsingException:
                pass
        raise ParsingException("Not exists event creator")
