import re
from itertools import chain
from common.log_parsing.metadata import ParsingException


class RegexpParser:
    """
    Parses event using regex
    """

    def __init__(self, pattern, match=True):
        """
        Creates regex parser
        :param pattern: regex pattern
        :param match: match needed flag
        """
        self.__pattern = re.compile(pattern)
        self.__match = match

    def parse(self, text):
        """
        Parses text using pattern
        :param text: input text
        :return: parsed list
        :raises: ParsingException when regex hasn't matched
        """
        if not self.__match or self.__pattern.match(text):
            return list(chain.from_iterable(self.__pattern.findall(text)))
        else:
            raise ParsingException("Text does not match the pattern.")
