import json
import six

from common.log_parsing.metadata import ParsingException
from common.spark_utils.custom_functions import flatten_json


class JsonParser:

    def __init__(self, keys_mapper=None, values_mapper=None, flatten=False, delimiter='.', fields_to_flat=None):
        self.__keys_mapper = keys_mapper
        self.__values_mapper = values_mapper
        self.__flatten = flatten
        self.__fields_to_flat = fields_to_flat

        if not isinstance(delimiter, six.string_types):
            raise ParsingException("Can't parse json!")

        self.__delimiter = delimiter

    def parse(self, text):
        """
        Parses text as json string
        :param text: input text
        :return: parsed dict
        :raises: ParsingException when text doesn't suit json format
        """
        if not isinstance(text, dict):
            try:
                text = json.loads(text)
            except (ValueError, TypeError):
                raise ParsingException("Can't parse message!")

        if self.__keys_mapper is not None and callable(self.__keys_mapper):
            text = dict(
                map(lambda kv: (self.__keys_mapper(kv[0]), kv[1]), text.iteritems()))

        if self.__values_mapper is not None and callable(self.__values_mapper):
            text = dict(
                map(lambda kv: (kv[0], self.__values_mapper(kv[1])), text.iteritems()))

        if self.__flatten:
            text = flatten_json(text, self.__delimiter, self.__fields_to_flat)

        return text
