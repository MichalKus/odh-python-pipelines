"""Module for SplitterParser"""


class SplitterParser:
    """
    Parses event using split function
    """

    def __init__(self, delimiter, is_trim=False, max_split=None):
        """
        Creates instances for split parsing
        :param delimiter: delimiter
        :param is_trim: flag to trim or not fields
        """
        self._delimiter = delimiter
        self._is_trim = is_trim
        self._max_split = max_split

    def parse(self, text):
        """
        Parses text using split function
        :param text: input text
        :return: parsed list
        """
        if self._max_split:
            result = text.split(self._delimiter, self._max_split)
        else:
            result = text.split(self._delimiter)
        return map(lambda item: item.strip(), result) if self._is_trim else result
