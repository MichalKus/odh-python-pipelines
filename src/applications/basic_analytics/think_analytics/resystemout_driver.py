"""
The module for the driver to calculate metrics related to Think Analytics resystemout component.
"""

from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Avg
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ThinkAnalyticsReSystemOut(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Think Analytics resystemout component.
    """

    def _process_pipeline(self, read_stream):
        duration_stream = read_stream \
            .where("level == 'INFO'") \
            .withColumn("duration", regexp_extract("message", r"^.*?(\d+)ms.$", 1).cast("Int").alias("duration")) \
            .aggregate(
            Avg(group_fields=["hostname"], aggregation_field="duration", aggregation_name=self._component_name))

        return [duration_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("script", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ThinkAnalyticsReSystemOut(configuration, ThinkAnalyticsReSystemOut.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
