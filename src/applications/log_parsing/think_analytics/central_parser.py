from common.log_parsing.list_event_creator.csv_parser import CsvParser


class CentralParser(object, CsvParser):
    """
    Extended parser for central logs
    """
    def __init__(self, delimiter=None, quotechar=None, skipinitialspace=False):
        super(CentralParser, self).__init__()
        self._delimiter = delimiter
        self._quotechar = quotechar
        self._skipinitialspace = skipinitialspace

    def parse(self, text):
        """
        Extended parsing with concat of timestamp
        :param text: Text of all message
        :return:
        """
        values = super(CentralParser, self).parse(text)
        self.concat_timestamp(values)
        return values

    @staticmethod
    def concat_timestamp(values):
        """
        Create timestamp from time and date
        :param values: all parsed fields
        """
        timestamp = values[0] + " " + values[1]
        values[0] = timestamp
        del values[1]

