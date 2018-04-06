"""
Module for PredicateEventCreator class
"""


class PredicateEventCreator(object):
    """
    Creates events for multiple match_fields
    """

    def __init__(self, fields, match_fields, full_match=False):
        """
        :param fields: fields that should match
        :param match_fields: match values and related event creator or dictionary that
        should appear in result if fields values match are matched
        :param full_match:
        :exception Exception
        """
        for keys, values in match_fields:
            if len(keys) != len(fields):
                raise ValueError("Fields count must be equal to matching fields count!")

        self.__match_fields = match_fields
        self.__fields = fields
        self.__full_match = full_match

    def create(self, row):
        """
        Creates events according to matched event creator or dictionary.
        :param row: input row.
        :return: map representing event where key is event field name and value is field value.
        """
        result = {}
        for values, event_creator in self.__match_fields:
            if self.__fields_match(values, row):
                if isinstance(event_creator, dict):
                    result.update(event_creator)
                else:
                    result.update(event_creator.create(row))
        return result

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
