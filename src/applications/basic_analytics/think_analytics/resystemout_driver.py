"""
The module for the driver to calculate metrics related to Think Analytics resystemout component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Avg, Count
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.functions import regexp_extract


class ThinkAnalyticsReSystemOutEventProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Think Analytics resystemout component.
    """

    def _process_pipeline(self, read_stream):

        re_system_out_stream = read_stream \
            .select("*")

        duration_stream = re_system_out_stream\
            .withColumn("duration", regexp_extract("message", r"^.*?(\d+)ms.$", 1).cast("Int").alias("duration"))

        return [self.__avg_duration(duration_stream),
                self.__count_by_level(re_system_out_stream)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("script", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])

    def __avg_duration(self, read_stream):
        return read_stream\
            .where("level == 'INFO'") \
            .aggregate(Avg(group_fields=["hostname"],
                           aggregation_field="duration",
                           aggregation_name=self._component_name))

    def __count_by_level(self, read_stream):
            return read_stream \
                .aggregate(Count(group_fields=["level"],
                                 aggregation_name=self._component_name))

    def __count_by_message(self, read_stream):
        return read_stream \
            .where("level == 'ERROR'") \
            .aggregate(Count(group_fields=["message"],
                             aggregation_name=self._component_name))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ThinkAnalyticsReSystemOutEventProcessor(configuration,
                                                   ThinkAnalyticsReSystemOutEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
