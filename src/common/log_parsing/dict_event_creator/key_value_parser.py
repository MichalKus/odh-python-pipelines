from common.log_parsing.metadata import ParsingException
from common.spark_utils.custom_functions import convert_to_underlined


class KeyValueParser:

    def __init__(self, sequence_separator, key_value_separator,
                 keys_strip=None, values_strip=None, skip_parsing_exceptions=False):
        self.__sequence_separator = sequence_separator
        self.__key_value_separator = key_value_separator
        self.__keys_strip = keys_strip
        self.__values_strip = values_strip
        self.__skip_parsing_exceptions = skip_parsing_exceptions

    def __trim_and_transform_to_key_value(self, key_value):
        pair = key_value.split(self.__key_value_separator)
        if (len(pair) != 2 or pair[0].isspace()) and not self.__skip_parsing_exceptions:
            raise ParsingException("Can't parse message!")
        return convert_to_underlined(pair[0].strip(self.__keys_strip)), pair[1].strip(self.__values_strip)

    def parse(self, text):
        """
        Parses text using pattern
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when text doesn't confirm key/value format
        """
        return dict(filter(lambda value: value[1],
                           map(self.__trim_and_transform_to_key_value, text.split(self.__sequence_separator))))
