from common.log_parsing.dict_event_creator.event_creator import EventCreator


class CridEventCreator(EventCreator):
    """
    Extended EventCreator with parsing URL inside message
    """

    def create(self, row):
        """
        Parse row with Parser and then parse URL
        :param row: Row from kafka topic
        :return: list of all fields with all URL parameters
        """
        values = EventCreator.create(self, row)
        if values:
            self.revert_escaped_symbols(values)
        return values

    @staticmethod
    def revert_escaped_symbols(values):
        values.update({"crid": values["crid"].replace("~~3A", ":").replace("~~2F", "/")})
