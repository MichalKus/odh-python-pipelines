from abc import ABCMeta, abstractmethod

from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType, DoubleType


class Aggregation:
    """
    Abstract class to define any aggregation for an input dataframe
    using specified fields to group and window if it's needed.
    """

    __metaclass__ = ABCMeta

    def __init__(self, group_fields=None, aggregation_field=None, aggregation_window=None, aggregation_name=None):
        self.__group_fields = [] if group_fields is None else \
            group_fields if isinstance(group_fields, list) else [group_fields]
        self._aggregation_field = aggregation_field
        self.__aggregation_window = aggregation_window
        self.__aggregation_name = aggregation_name

    def apply(self, input_dataframe, aggregation_window=None):
        actual_window = self.__aggregation_window \
            if self.__aggregation_window is not None else aggregation_window
        metric_name_list = self.__construct_metric_name(input_dataframe)

        return self.aggregate(
            input_dataframe.groupBy(window("@timestamp", actual_window), *self.__group_fields)
            if actual_window is not None else input_dataframe.groupBy(*self.__group_fields)
        ).withColumn("metric_name", concat_ws(".", *metric_name_list))

    def __construct_metric_name(self, input_dataframe):
        return [lit(self.__aggregation_name)] + \
               [concat(lit(group_field), lit("."), regexp_replace(input_dataframe[group_field], "\\s+", "_"))
                for group_field in self.__group_fields] + \
               [lit(self._aggregation_field)] + \
               [lit(self.__class__.__name__.lower())]

    @abstractmethod
    def aggregate(self, grouped_dataframe):
        """
        Abstract aggregation
        :param grouped_dataframe: input dataframe grouped by specified fields
        :return: Aggregated dataframe
        """


class AggregatedDataFrame:
    """
    Class for aggregated dataframe by specified functions.
    """

    def __init__(self, dataframe, aggregations):
        self.__dataframe = dataframe
        self.__aggregations = aggregations if isinstance(aggregations, list) else [aggregations]

    def results(self, window=None):
        return [aggregation.apply(self.__dataframe, window) for aggregation in self.__aggregations]


class Avg(Aggregation):
    """
    Computes average values for each numeric columns for each group.
    """

    def aggregate(self, grouped_dataframe): return grouped_dataframe.agg(
        avg(self._aggregation_field).cast(DecimalType(scale=2)).alias("value"))


class Min(Aggregation):
    """
    Computes the min value for each numeric column for each group.
    """

    def aggregate(self, grouped_dataframe): return grouped_dataframe.agg(min(self._aggregation_field).alias("value"))


class Max(Aggregation):
    """
    Computes the max value for each numeric column for each group.
    """

    def aggregate(self, grouped_dataframe): return grouped_dataframe.agg(max(self._aggregation_field).alias("value"))


class Sum(Aggregation):
    """
    Compute the sum for each numeric columns for each group.
    """

    def aggregate(self, grouped_dataframe): return grouped_dataframe.agg(sum(self._aggregation_field).alias("value"))


class Count(Aggregation):
    """
    Counts the number of records for each group.
    """

    def aggregate(self, grouped_dataframe): return grouped_dataframe.agg(count("*").alias("value"))