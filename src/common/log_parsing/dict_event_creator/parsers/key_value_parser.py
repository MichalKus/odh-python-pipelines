"""Module for key/value parser"""
from common.log_parsing.metadata import ParsingException
from common.spark_utils.custom_functions import convert_to_underlined


class KeyValueParser(object):
    """Parser for strings that contain key/value sequence"""
    def __init__(self, sequence_separator, key_value_separator,
                 keys_mapper=None, values_mapper=None,
                 skip_parsing_exceptions=False):
        self.__sequence_separator = sequence_separator
        self.__key_value_separator = key_value_separator
        self.__keys_mapper = keys_mapper
        self.__values_mapper = values_mapper
        self.__skip_parsing_exceptions = skip_parsing_exceptions

    def __trim_and_transform_to_key_value(self, key_value):
        pair = key_value.split(self.__key_value_separator)
        if len(pair) != 2 or pair[0].isspace():
            if self.__skip_parsing_exceptions:
                return None
            else:
                raise ParsingException("Can't parse message!")

        key = self.__keys_mapper(pair[0].strip()) \
            if self.__keys_mapper is not None and callable(self.__keys_mapper) else pair[0].strip()

        value = self.__values_mapper(pair[1].strip()) \
            if self.__values_mapper is not None and callable(self.__values_mapper) else pair[1].strip()

        return convert_to_underlined(key), value

    def parse(self, text):
        """
        Parses text as key/value sequence
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when text doesn't confirm key/value format
        """
        return dict(filter(lambda value: value[1],
                           filter(lambda x: x is not None,
                                  map(self.__trim_and_transform_to_key_value, text.split(self.__sequence_separator)))))
