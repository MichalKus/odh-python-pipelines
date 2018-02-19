import re

from pyspark.sql.functions import col, lower, when, Column, from_unixtime
from pyspark.sql.types import TimestampType


def custom_translate_regex(source_field, mapping, default_value, exact_match=False):
    if not isinstance(source_field, Column):
        raise TypeError("source_field should be a Column")

    if not isinstance(mapping, dict):
        raise TypeError("mapping should be a dict")

    if mapping is None:
        raise TypeError("mapping should not be None")

    def exact_match_condition(key):
        return source_field == key

    def regexp_match_condition(key):
        return source_field.rlike(key)

    condition = lambda key: exact_match_condition(key) \
        if exact_match \
        else regexp_match_condition(key)

    callable = None
    for key, value in mapping.items():
        if callable is None:
            callable = when(condition(key), value)
        else:
            callable = callable.when(condition(key), value)

    return callable.otherwise(default_value)


def custom_translate_like(source_field, mappings_pair, default_value):
    """
    This function returns function which can translate column values to values specified by mapping.
    Mapping is spacified as list of tuples. Each tuple contains list of match strings and value to return if
    match strings are found in column values.
    :param source_field: field to analyze
    :param mappings_pair: list of matching tuples.
    :param default_value: spark sql value for column if mapping is not found.
    :return: complex spark sql function which can be used in select or withColumns.
    """

    def get_like_condition(source_filed, mappings):
        """
        This function returns a part of matching expression.
        :param source_filed: source column
        :param mappings: array of string values to be looked for in source column.
        :return: matching expression
        """
        return reduce(
            lambda c1, c2: c1.__and__(c2),
            [lower(source_filed).like("%" + mapping.lower() + "%") for mapping in mappings]
        )

    result = None
    for mappings, value in mappings_pair:
        if result is None:
            result = when(get_like_condition(source_field, mappings), value)
        else:
            result = result.when(get_like_condition(source_field, mappings), value)

    return result.otherwise(default_value)


def convert_epoch_to_iso(data_stream, input_field_name, result_field_name):
    """
    This function converts the epoch input timestamp to spark TimestampType
    :param data_stream: input spark dataframe
    :param input_field_name: input column name
    :param result_field_name: result column name
    :return: dataframe with added TimesatmpType column
    """
    return data_stream.withColumn(result_field_name, from_unixtime(col(input_field_name) / 1000).cast(TimestampType()))


def convert_to_underlined(text):
    """
    The function to convert camel-cased text to underlined
    :param text: input text to convert
    :return: underlined text
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
