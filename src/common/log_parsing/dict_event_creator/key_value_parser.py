import re
from common.log_parsing.metadata import ParsingException


class KeyValueParser:

    def __init__(self):
        pass

    @staticmethod
    def parse(text):
        """
        Parses text using pattern
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when text doesn't confirm key/value format
        """

        def __trim_and_transform_to_key_value(key_value):
            pair = key_value.split("=")
            if len(pair) != 2 | pair[0].isspace():
                raise ParsingException("Can't parse message!")
            return re.sub('(?<!^)(?=[A-Z])', '_', pair[0].strip()).lower(), pair[1].strip()

        return dict(filter(lambda value: value[1], map(__trim_and_transform_to_key_value, text.split(","))))
