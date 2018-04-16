"""Module for SplitterParser"""


class SplitterParser:
    """
    Parses event using split function
    """

    def __init__(self, delimiter, is_trim=False):
        """
        Creates instances for split parsing
        :param delimiter: delimiter
        :param is_trim: flag to trim or not fields
        """
        self._delimiter = delimiter
        self._is_trim = is_trim

    def parse(self, text):
        """
        Parses text using split function
        :param text: input text
        :return: parsed list
        """
        result = text.split(self._delimiter)
        return map(lambda item: item.strip(), result) if self._is_trim else result
