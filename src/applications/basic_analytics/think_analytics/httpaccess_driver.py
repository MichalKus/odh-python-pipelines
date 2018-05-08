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

        return [self.__avg_response_time_by_method_stream(read_stream),
                self.__count_by_method_stream(read_stream),
                self.__avg_response_time_stream(read_stream),
                self.__count_responses_stream(read_stream),
                self.__count_by_code_stream(read_stream)]

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
            StructField("traxis-profile-id", StringType()),
            StructField("hostname", StringType())
        ])

    def __avg_response_time_by_method_stream(self, read_stream):
        return read_stream \
            .where("method is not null") \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(Avg(group_fields=["hostname", "method"],
                           aggregation_field="response_time",
                           aggregation_name=self._component_name))

    def __count_by_method_stream(self, read_stream):
        return read_stream \
            .where("method is not null") \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(Count(group_fields=["hostname", "method"], aggregation_name=self._component_name))

    def __avg_response_time_stream(self, read_stream):
        return read_stream \
            .withColumn("response_time", read_stream["response_time"].cast("Int")) \
            .aggregate(Avg(group_fields=["hostname"],
                           aggregation_field="response_time",
                           aggregation_name=self._component_name))

    def __count_responses_stream(self, read_stream):
        return read_stream \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".responses"))

    def __count_by_code_stream(self, read_stream):
        return read_stream \
            .where("response_code is not null") \
            .aggregate(Count(group_fields=["hostname", "response_code"],
                             aggregation_name=self._component_name))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ThinkAnalyticsHttpAccess(configuration, ThinkAnalyticsHttpAccess.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
