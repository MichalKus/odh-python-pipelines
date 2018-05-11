"""
The module for the driver to calculate metrics related to Traxis Backend error component.
"""

from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisBackendError(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Backend error component.
    """

    def _process_pipeline(self, read_stream):
        error_events = read_stream.where("level == 'ERROR'")

        return [self.__error_count(error_events),
                self.__cassandra_errors(error_events),
                self.__undefined_errors(error_events),
                self.__error_count_per_host_names(error_events)]

    def __error_count(self, error_events):
        return error_events \
            .aggregate(Count(aggregation_name=self._component_name))

    def __cassandra_errors(self, error_events):
        return error_events \
            .where("message like '%Exception with cassandra node%'") \
            .withColumn("host", regexp_extract("message",
                                               r".*Exception\s+with\s+cassandra\s+node\s+\'([\d\.]+).*", 1)
                        ) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".cassandra_errors"))

    def __undefined_errors(self, error_events):
        return error_events \
            .where("message not like '%Exception with cassandra node%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".undefined_errors"))

    def __error_count_per_host_names(self, error_events):
        return error_events \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".error_event"))

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TraxisBackendError(configuration, TraxisBackendError.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
