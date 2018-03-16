from common.log_parsing.metadata import ParsingException


class KeyValueParser:

    def __init__(self, sequence_separator, key_value_separator, key_transformer):
        self.__sequence_separator = sequence_separator
        self.__key_value_separator = key_value_separator
        self.__key_transformer = key_transformer

    def __trim_and_transform_to_key_value(self, key_value):
        pair = key_value.split(self.__key_value_separator)
        if len(pair) != 2 | pair[0].isspace():
            raise ParsingException("Can't parse message!")
        return self.__key_transformer.transform(pair[0]), pair[1].strip()

    def parse(self, text):
        """
        Parses text using pattern
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when text doesn't confirm key/value format
        """
        return dict(filter(lambda value: value[1], map(self.__trim_and_transform_to_key_value, text.split(self.__sequence_separator))))
