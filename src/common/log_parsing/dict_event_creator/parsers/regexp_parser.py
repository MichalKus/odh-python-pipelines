"""Regexp Parser"""
import re

from common.log_parsing.metadata import ParsingException


class RegexpParser(object):
    """
    Parses event using regex and named groups
    """

    def __init__(self, pattern, return_empty_dict=False):
        """
        Creates regex parser with named groups
        :param pattern: regex pattern
        :param return_empty_dict: define if we need empty dict rather exception
        """
        self.__pattern = re.compile(pattern)
        self.__return_empty_dict = return_empty_dict

    def parse(self, text):
        """
        Parses text using pattern
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when regex hasn't matched
        """
        match = self.__pattern.match(text)
        if match:
            return match.groupdict()
        elif self.__return_empty_dict:
            return {}
        else:
            raise ParsingException("Text does not match the pattern.")
