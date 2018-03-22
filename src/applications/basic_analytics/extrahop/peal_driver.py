"""
The module for the driver to calculate metrics related to extrahop peal data.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, DoubleType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ExtrahopPeal(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to extrahop peal data.
    """

    def _process_pipeline(self, read_stream):
        count_by_payload_status_stream = read_stream \
            .aggregate(Count(group_fields=["payload_status"],
                             aggregation_name=self._component_name))

        count_by_status_stream = read_stream \
            .aggregate(Count(group_fields=["status"],
                             aggregation_name=self._component_name))

        avg_latency_by_uri_stream = read_stream \
            .aggregate(Avg(group_fields=["uri"],
                           aggregation_field="latency",
                           aggregation_name=self._component_name))

        count_by_uri_and_status_stream = read_stream \
            .aggregate(Count(group_fields=["uri", "status"],
                             aggregation_name=self._component_name))

        return [count_by_payload_status_stream, count_by_status_stream,
                avg_latency_by_uri_stream, count_by_uri_and_status_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("latency", DoubleType()),
            StructField("payload_status", StringType()),
            StructField("uri", StringType()),
            StructField("status", StringType()),
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ExtrahopPeal(configuration, ExtrahopPeal.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
