from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.metadata import ParsingException


class EventIngestCreator(EventCreator):
    """
    Extended EventCreator with checking for equals of started and finished scripts
    """

    def __init__(self, metadata, parser, matcher=None, field_to_parse="message", timezone_field="tz"):
        self._metadata = metadata
        self._parser = parser
        EventCreator.__init__(self, metadata, parser, matcher, field_to_parse, timezone_field)

    def create(self, row):
        """
        Parse row with Parser and then check for equals of started and finished script names
        :param row: Row from kafka topic
        :return: list of all fields
        """
        values = super(EventIngestCreator, self).create(row)
        if values["started_script"] == values["finished_script"]:
            duration = abs(values["finished_time"] - values["@timestamp"]).seconds
            values.update({"duration": duration})
            return values
        else:
            raise ParsingException("Message contains different started and finished scripts")
