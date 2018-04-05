import re
from common.log_parsing.dict_event_creator.event_creator import EventCreator


class CridEventCreator(EventCreator):
    """
    Extended EventCreator with parsing influx metric str inside msg
    """

    def create(self, row):
        """
        Parse row with Parser and then parse message & crid
        :param row: Row from kafka topic
        :return: list of all fields with all URL parameters
        """
        values = EventCreator.create(self, row)
        if values:
            self.custom_dict(values)
        return values

    @staticmethod
    def custom_dict(values):
        """
        extract crid from message
        :param values: input dict
        """
        message = values["message"]
        if 'crid' in message:
            crid = re.search(r"(?s)(?P<crid>crid[^\\]*)", message).group(0)
            values.update({"crid": crid})
