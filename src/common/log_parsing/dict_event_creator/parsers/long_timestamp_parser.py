"""Module for longTimestampParser"""
import datetime

from common.log_parsing.metadata import ParsingException


class LongTimestampParser(object):
    """
    Parses timestamp field from long value
    """

    def __init__(self, field, return_empty_dict=False):
        """
        Parser constructor
        :param field: field name for timestamp
        :param return_empty_dict: define if we need empty dict rather exception
        """
        self.__field = field
        self.__return_empty_dict = return_empty_dict

    def parse(self, text):
        """
        Parses text using pattern
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when text is not parsed
        """
        try:
            long_timestamp = long(text)
            return {
                self.__field: datetime.datetime.utcfromtimestamp(long_timestamp).strftime('%Y-%m-%d %H:%M:%S')
            }
        except ValueError:
            if self.__return_empty_dict:
                return {}
            else:
                raise ParsingException("Wrong long timestamp format")
