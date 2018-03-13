"""
The module to define aggregations for an input dataframes.
Aggregation class defines common methods for all aggregations.
"""

from abc import ABCMeta, abstractmethod

from pyspark.sql.functions import lit, avg, count, sum, max, min, col, stddev, expr, regexp_replace
from pyspark.sql.functions import window, concat_ws, approx_count_distinct, explode, create_map, when
from pyspark.sql.types import DecimalType
from itertools import chain

from common.spark_utils.custom_functions import convert_to_underlined


class Aggregation(object):
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

    def apply(self, input_dataframe, aggregation_window, time_column):
        actual_window = self.__aggregation_window \
            if self.__aggregation_window is not None else aggregation_window
        window_aggreagated_df = input_dataframe.groupBy(window(time_column, actual_window), *self.__group_fields)
        aggregated_dataframe = self.aggregate(window_aggreagated_df)
        if "metric_name" not in aggregated_dataframe.columns:
            aggregated_dataframe = self._add_metric_name_column(aggregated_dataframe)
        return aggregated_dataframe

    def _add_metric_name_column(self, df, suffix=None):
        metric_name_parts = [lit(self.__aggregation_name)]

        for group_field in self.__group_fields:
            metric_name_parts += [lit(group_field)]
            metric_name_parts += [group_field]

        metric_name_parts += [lit(self._aggregation_field)]
        metric_name_parts += [lit(self.get_name())] if suffix is None else [suffix]

        metric_name = concat_ws(".", *[(regexp_replace(x, "[\\s\\.]+", "_")) if isinstance(x, basestring)
                                       else regexp_replace(x, "\\s+", "_") for x in metric_name_parts])

        df = df.withColumn("metric_name", metric_name)\
            .withColumn("metric_name", regexp_replace("metric_name", "\\.\\.", ".EMPTY."))
        return df

    def aggregate(self, grouped_dataframe):
        """
        Abstract aggregation
        :param grouped_dataframe: input dataframe grouped by specified fields
        :return: Aggregated dataframe
        """
        result = grouped_dataframe.agg(self.get_aggregation().alias("value"))
        return self.post_process(result)

    @abstractmethod
    def get_aggregation(self):
        """
        Abstract aggregation
        :param grouped_dataframe: input dataframe grouped by specified fields
        :return: Aggregated dataframe
        """

    def post_process(self, df):
        """
        Callback method to apply any needed transformations to
        dataframe after aggregation column(s) are already included in it.
        Typically should be called from aggregate method.
        Default implementation does nothing.
        The method is not made abstract intentionally to avoid forcing developers to provide no-op implementations.
        :param df: dataframe to perform any needed transformations
        :return: Transformed dataframe
        """
        return df

    def get_name(self):
        return convert_to_underlined(self.__class__.__name__)


class AggregatedDataFrame(object):
    """
    Class for aggregated dataframe by specified functions.
    """

    def __init__(self, dataframe, aggregations):
        self.__dataframe = dataframe
        self.__aggregations = aggregations if isinstance(aggregations, list) else [aggregations]

    def results(self, aggregation_window, time_column):
        return [aggregation.apply(self.__dataframe, aggregation_window, time_column) for aggregation in self.__aggregations]


class Avg(Aggregation):
    """
    Computes average values for each numeric columns for each group.
    """
    def get_aggregation(self):
        return avg(self._aggregation_field).cast(DecimalType(scale=2))


class Min(Aggregation):
    """
    Computes the min value for each numeric column for each group.
    """
    def get_aggregation(self):
        return min(self._aggregation_field)


class Max(Aggregation):
    """
    Computes the max value for each numeric column for each group.
    """
    def get_aggregation(self):
        return max(self._aggregation_field)


class Sum(Aggregation):
    """
    Compute the sum for each numeric columns for each group.
    """
    def get_aggregation(self):
        return sum(self._aggregation_field)


class Count(Aggregation):
    """
    Counts the number of records for each group.
    """
    def get_aggregation(self):
        return count("*")


class DistinctCount(Aggregation):
    """
    Returns an approximate distinct count of ``_aggregation_field``
    """
    def get_aggregation(self):
        return approx_count_distinct(self._aggregation_field)


class Stddev(Aggregation):
    """
    Computes standard deviation
    """
    def get_aggregation(self):
        return stddev(self._aggregation_field)


class Percentile(Aggregation):
    """
    Computes percentiles
    """
    def get_aggregation(self):
        return expr("percentile(" + self._aggregation_field + ", array(" + str(self.percentile()) + "))")

    @abstractmethod
    def percentile(self):
        """
        Computes percentiles
        """

    def post_process(self, df):
        column_name = self.get_name()
        return df.withColumn(column_name, col(column_name).getItem(0))


class P01(Percentile):
    """
    Computes 0.01 percentile
    """
    def percentile(self):
        return 0.01


class P05(Percentile):
    """
    Computes 0.05 percentile
    """
    def percentile(self):
        return 0.05


class P10(Percentile):
    """
    Computes 0.1 percentile
    """
    def percentile(self):
        return 0.1


class P25(Percentile):
    """
    Computes 0.25 percentile
    """
    def percentile(self):
        return 0.25


class P50(Percentile):
    """
    Computes 0.5 percentile
    """
    def percentile(self):
        return 0.5


class Median(Percentile):
    """
    Computes median
    """
    def percentile(self):
        return 0.5


class P75(Percentile):
    """
    Computes 0.75 percentile
    """
    def percentile(self):
        return 0.75


class P90(Percentile):
    """
    Computes 0.9 percentile
    """
    def percentile(self):
        return 0.9


class P95(Percentile):
    """
    Computes 0.95 percentile
    """
    def percentile(self):
        return 0.95


class P99(Percentile):
    """
    Computes 0.99 percentile
    """
    def percentile(self):
        return 0.99


class CompoundAggregation(Aggregation):
    """
    Computes collection of aggregations in "one go"
    """
    def __init__(self, aggregations, group_fields=None, aggregation_field=None,
                 aggregation_window=None, aggregation_name=None):
        self.__aggregations = aggregations
        super(CompoundAggregation, self).__init__(group_fields, aggregation_field, aggregation_window, aggregation_name)

    def get_aggregation(self):
        return None

    def aggregate(self, grouped_dataframe):
        aggregation_names = [aggregation.get_name() for aggregation in self.__aggregations]
        aggregations = [aggregation.get_aggregation().alias(aggregation.get_name())
                        for aggregation in self.__aggregations]
        map_column = create_map(list(chain(*((lit(agg), col(agg)) for agg in aggregation_names)))).alias("map")
        result = grouped_dataframe.agg(*aggregations)
        for aggregation in self.__aggregations:
            result = aggregation.post_process(result)
        result = result\
            .withColumn("map", map_column)\
            .select("*", explode(col("map")).alias("metric_name", "value"))\
            .drop("map").drop(*aggregation_names)
        return self._add_metric_name_column(result, col("metric_name"))
