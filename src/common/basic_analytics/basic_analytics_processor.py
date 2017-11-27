from abc import ABCMeta, abstractmethod

from dateutil.tz import tz
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from common.basic_analytics.aggregations import AggregatedDataFrame


class BasicAnalyticsProcessor:
    """
    Pipeline for basic analytics
    """
    __metaclass__ = ABCMeta

    def __init__(self, configuration, schema):
        self.__configuration = configuration
        self.__schema = schema
        self._component_name = configuration.property("analytics.componentName")
        DataFrame.aggregate = BasicAnalyticsProcessor.__aggregate_dataframe

    @staticmethod
    def __aggregate_dataframe(dataframe, aggregations):
        return AggregatedDataFrame(dataframe, aggregations)

    @abstractmethod
    def _process_pipeline(self, json_stream):
        """
        Abstract  pipeline method
        :param json_stream: kafka stream reader
        :return: pipeline dataframe
        """

    def create(self, read_stream):
        """
        Abstract create pipeline method
        :param read_stream: kafka stream reader
        :return: pipeline dataframe
        """
        json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self.__schema).alias("json")) \
            .select("json.*") \
            .withWatermark("@timestamp", self.__get_interval_duration("watermark"))

        dataframes = self._process_pipeline(json_stream)
        aggregated_dataframes = dataframes if isinstance(dataframes, list) else [dataframes]

        # Check return types for abstract method
        if len(aggregated_dataframes) == 0 or \
                reduce(lambda f, d: f or not isinstance(d, AggregatedDataFrame),
                       aggregated_dataframes, False):
            raise SyntaxError("Method _process_pipeline has to return one object "
                              "with type AggregatedDataFrame or array of such objects.")

        # Get aggregations results
        actual_window = self.__get_interval_duration("window")
        aggregated_results = [df.results(actual_window) for df in aggregated_dataframes]
        return [self.__convert_to_kafka_structure(result) for results in aggregated_results for result in results]

    def __convert_to_kafka_structure(self, dataframe):
        return dataframe \
            .withColumn("@timestamp", col("window.start")) \
            .drop("window") \
            .selectExpr("metric_name AS key", "to_json(struct(*)) AS value")\
            .withColumn("topic", lit(self.__configuration.property("kafka.topics.output")))

    def __get_interval_duration(self, name):
        return self.__configuration.property("analytics." + name)
