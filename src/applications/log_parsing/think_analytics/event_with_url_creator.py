from common.log_parsing.list_event_creator.event_creator import EventCreator
import urlparse


class EventWithUrlCreator(EventCreator):
    """
    Extended EventCreator with parsing URL inside message
    """

    def __init__(self, metadata, parser, matcher=None, timezone_field="tz"):
        """
        Creates event creator with parsing URL
        :param metadata: metadata
        :param parser: list parser
        :param matcher: matcher object to check the input line to perform the parsing step only if the line is matched
        :param timezone_field: field name with information about timezone
        """
        EventCreator.__init__(self, metadata, parser, matcher, timezone_field)
        self._metadata = metadata
        self._parser = parser

    def create(self, row):
        """
        Parse row with Parser and then parse URL
        :param row: Row from kafka topic
        :return: list of all fields with all URL parameters
        """
        values = super(EventWithUrlCreator, self).create(row)
        self.split_url(values)
        return values

    @staticmethod
    def split_url(values):
        """
        Find field url and parse it
        :param values: List of fields after first split
        :return: list of all fields with all URL parameters
        """
        url = values["url"].split("?")
        del values["url"]
        if len(url) >= 2:
            allparameters = url[1]
            params = dict(urlparse.parse_qsl(allparameters))
            params.update({"action": url[0]})
            values.update(params)
        else:
            values.update({"action": url[0]})
