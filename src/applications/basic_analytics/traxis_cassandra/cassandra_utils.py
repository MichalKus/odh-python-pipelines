"""
Module that contains all Utility Classes for Cassandra Drivers
"""
from common.basic_analytics.aggregations import Count
from common.spark_utils.custom_functions import custom_translate_like
from pyspark.sql.functions import col


class CassandraUtils(object):
    """
    Utility class for Cassandra drivers with common static methods for calculating Cassandra metrics
    """
    def __init__(self):
        pass

    @staticmethod
    def memory_flushing(events, component_name):
        return events \
            .where("message like '%Flushing%'") \
            .withColumn("column_family", custom_translate_like(
                source_field=col("message"),
                mappings_pair=[(["Channels"], "channels"),
                               (["Titles"], "titles"),
                               (["Groups"], "groups")],
                default_value="unclassified")) \
            .where("column_family != 'unclassified'") \
            .aggregate(Count(group_fields=["column_family"],
                             aggregation_name=component_name + ".memory_flushing"))
