"""
Module for PredicateEventCreator class
"""
from common.log_parsing.metadata import AbstractEventCreator


class PredicateEventCreator(AbstractEventCreator):
    """
    Creates events for multiple match fields
    """

    def __init__(self, value_type, fields, match_fields, full_match=False):
        """
        :param value_type: value type
        :param fields: fields that should match
        :param match_fields: values that should match with fields values
        :param full_match:
        :exception Exception
        """
        for keys, values in match_fields:
            if len(keys) != len(fields):
                raise ValueError("Fields count must be equal to matching fields count!")

        AbstractEventCreator.__init__(self, None, None, None)
        self.__match_fields = match_fields
        self.__value_type = value_type
        self.__fields = fields
        self.__full_match = full_match

    def _create_with_context(self, row, context):
        """
        Converts row.
        :param row: input row.
        :param context: dictionary with additional data.
        :return: map representing event where key is event field name and value is field value.
        """
        result = None
        for values, match_result in self.__match_fields:
            if self.__fields_match(values, row):
                result = match_result
        return {self.__value_type.get_output_name(): result} if result else {}

    def __fields_match(self, values, row):
        dictionary = zip(self.__fields, values)
        for field, value in dictionary:
            if self.__full_match:
                if value != row.get(field):
                    return False
            else:
                if not row.get(field) or value not in row.get(field):
                    return False
        return True

    def contains_fields_to_parse(self, row):
        """
        Checks if row contains fields to parse
        :param row: input row
        :return: boolean
        """
        return set(self.__fields).issubset(row.keys())
