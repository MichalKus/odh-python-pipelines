"""
The module for the driver to calculate metrics related to Think Analytics HTTP access component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from pyspark.sql.functions import col, lit, when, udf
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class CatalinaConnectionEventsProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Think Analytics HTTP access component.
    """

    def _process_pipeline(self, read_stream):
        count_by_method_stream = read_stream \
            .where(col("event_type") == "NETWORK" and col("message").contains("connection") and col("message")) \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(Count(group_fields=["hostname", "method"], aggregation_name=self._component_name))


        count_responses_stream = read_stream \
            .aggregate(Count(group_fields=["hostname"], aggregation_name=self._component_name + ".responses"))

        count_by_code_stream = read_stream \
            .where("response_code is not null") \
            .aggregate(Count(group_fields=["hostname", "response_code"], aggregation_name=self._component_name))

        return [ count_by_method_stream, count_responses_stream, count_by_code_stream]

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
    return CatalinaConnectionEventsProcessor(configuration, CatalinaConnectionEventsProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
