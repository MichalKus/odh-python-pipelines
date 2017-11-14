from common.log_parsing.list_event_creator.splitter_parser import SplitterParser


class HttpAccessParser(object, SplitterParser):
    """
    Extended parser for http_access logs
    """
    def __init__(self, delimiter=None, is_trim=False):
        super(HttpAccessParser, self).__init__()
        self._delimiter = delimiter
        self._is_trim = is_trim

    def parse(self, text):
        """
        Extended parsing with concat of timestamp
        :param text: Text of all message from kafka
        :return:
        """
        values = super(HttpAccessParser, self).parse(text)
        self.concat_timestamp(values)
        return values

    @staticmethod
    def concat_timestamp(values):
        """
        Create timestamp from time and timezone
        :param values: all parsed fields
        """
        timestamp = values[0] + values[1]
        timestamp = timestamp[1:-1]
        values[0] = timestamp
        del values[1]

