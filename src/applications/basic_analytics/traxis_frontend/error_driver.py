"""
The module for the driver to calculate metrics related to Traxis Frontend error component.
"""

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_like
from common.basic_analytics.aggregations import Count
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisFrontendError(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Frontend error component.
    """

    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == 'ERROR'")

        return [self.__count(info_events.union(warn_events), "info_or_warn"),
                self.__count(error_events, "error"),
                self.__counts_by_level_and_hostname(read_stream),
                self.__error_metrics(read_stream)]

    def __count(self, events, metric_name):
        return events \
            .aggregate(Count(aggregation_name="{0}.{1}".format(self._component_name, metric_name)))

    def __counts_by_level_and_hostname(self, events):
        return events \
            .aggregate(Count(group_fields=["level", "hostname"], aggregation_name=self._component_name))

    def __error_metrics(self, events):
        error_stream = events.where("level = 'ERROR'") \
            .withColumn("counter",
                        custom_translate_like(
                            source_field=col("message"),
                            mappings_pair=[
                                (["Eventis.Traxis.Cassandra.CassandraException"], "traxis_cassandra_error"),
                                (["NetworkTimeCheckError"], "ntp_error")
                            ],
                            default_value="unclassifed_errors"))

        warn_and_fatal_stream = events.where("level in ('WARN', 'FATAL')") \
            .withColumn("counter", lit("unclassifed_errors"))

        return error_stream.union(warn_and_fatal_stream) \
            .aggregate(Count(group_fields=["hostname", "counter"],
                             aggregation_name=self._component_name))

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("thread_name", StringType()),
            StructField("component", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TraxisFrontendError(configuration, TraxisFrontendError.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
