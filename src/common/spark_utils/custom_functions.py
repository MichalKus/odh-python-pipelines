from pyspark.sql.functions import *
from pyspark.sql.functions import Column


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
    def get_like_condition(source_filed, mappings):
        return  reduce(
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
