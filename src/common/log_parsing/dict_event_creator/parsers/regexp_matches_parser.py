import re
from common.log_parsing.metadata import ParsingException


class RegexpMatchesParser:
    """
    Parses event using regex finditer
    """

    def __init__(self, pattern):
        """
        Creates finditer parser
        :param pattern: regex pattern
        """
        self.__pattern = pattern

    def parse(self, text):
        """
        Parses text using pattern, return first match
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when regex hasn't matched
        """
        matches = re.finditer(self.__pattern, text)
        for match in matches:
            return match.groupdict()
        else:
            raise ParsingException("Text does not match the pattern.")
