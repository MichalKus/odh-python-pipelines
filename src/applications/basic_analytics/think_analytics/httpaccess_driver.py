"""
The module for the driver to calculate metrics related to Think Analytics HTTP access component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ThinkAnalyticsHttpAccess(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Think Analytics HTTP access component.
    """

    def _process_pipeline(self, read_stream):
        avg_response_time_by_method_stream = read_stream \
            .where("method is not null") \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(Avg(group_fields=["hostname", "method"], aggregation_field="response_time",
                           aggregation_name=self._component_name))

        count_by_method_stream = read_stream \
            .where("method is not null") \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(Count(group_fields=["hostname", "method"], aggregation_name=self._component_name))

        avg_response_time_stream = read_stream \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(
            Avg(group_fields=["hostname"], aggregation_field="response_time", aggregation_name=self._component_name))

        count_responses_stream = read_stream \
            .aggregate(Count(group_fields=["hostname"], aggregation_name=self._component_name + ".responses"))

        count_by_code_stream = read_stream \
            .where("response_code is not null") \
            .aggregate(Count(group_fields=["hostname", "response_code"], aggregation_name=self._component_name))

        return [avg_response_time_by_method_stream, count_by_method_stream, avg_response_time_stream,
                count_responses_stream, count_by_code_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("ip", StringType()),
            StructField("thread", StringType()),
            StructField("http_method", StringType()),
            StructField("http_version", StringType()),
            StructField("response_code", StringType()),
            StructField("response_time", StringType()),
            StructField("contentSourceId", StringType()),
            StructField("clientType", StringType()),
            StructField("method", StringType()),
            StructField("subscriberId", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ThinkAnalyticsHttpAccess(configuration, ThinkAnalyticsHttpAccess.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
