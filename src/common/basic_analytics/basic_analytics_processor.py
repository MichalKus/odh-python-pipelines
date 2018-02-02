""""
Modules contains superclass for basic analytics processors.
It implements processing pipeline.
"""
from abc import ABCMeta, abstractmethod

from pyspark.sql.functions import from_json, lit, col
from pyspark.sql import DataFrame
from common.basic_analytics.aggregations import AggregatedDataFrame


class BasicAnalyticsProcessor(object):
    """
    Pipeline for basic analytics.
    """
    __metaclass__ = ABCMeta

    def __init__(self, configuration, schema, timefield_name="@timestamp", drop_columns=[]):
        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration.property("analytics.componentName")
        self._timefield_name = timefield_name
        self._drop_columns = drop_columns
        DataFrame.aggregate = BasicAnalyticsProcessor.__aggregate_dataframe

    @staticmethod
    def __aggregate_dataframe(dataframe, aggregations):
        return AggregatedDataFrame(dataframe, aggregations)

    def _prepare_stream(self, read_stream):
        data_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*")
        return self._prepare_timefield(data_stream).withWatermark(self._timefield_name,
                                                                  self._get_interval_duration("watermark"))

    def _prepare_timefield(self, data_stream):
        """
        This method can be overridden to create proper time field in data_stream. This may need if time field name
        is not @timestamp.
        Default implementation does nothing and it's consistent with default name of time field '@timestamp'.
        :param data_stream: spark stream
        :return: spark stream
        """
        return data_stream

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
        json_stream = self._prepare_stream(read_stream)
        dataframes = self._process_pipeline(json_stream)
        aggregated_dataframes = dataframes if isinstance(dataframes, list) else [dataframes]

        # Check return types for abstract method
        if len(aggregated_dataframes) == 0 or \
                reduce(lambda f, d: f or not isinstance(d, AggregatedDataFrame),
                       aggregated_dataframes, False):
            raise SyntaxError("Method _process_pipeline has to return one object "
                              "with type AggregatedDataFrame or array of such objects.")

        # Get aggregations results
        aggregation_window = self._get_interval_duration("window")
        aggregated_results = [df.results(aggregation_window, self._timefield_name) for df in aggregated_dataframes]
        return [self._convert_to_kafka_structure(result) for results in aggregated_results for result in results]

    def _convert_to_kafka_structure(self, dataframe):
        drop_list = ["window"]
        drop_list.extend(self._drop_columns)
        return dataframe \
            .withColumn("@timestamp", col("window.start")) \
            .drop(*drop_list) \
            .selectExpr("metric_name AS key", "to_json(struct(*)) AS value") \
            .withColumn("topic", lit(self.__configuration.property("kafka.topics.output")))

    def _get_interval_duration(self, name):
        return self.__configuration.property("analytics." + name)
