"""
The module for the driver to calculate metrics related to Think Analytics reingest component.
"""

from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Avg
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ThinkAnalyticsReIngest(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Think Analytics reingest component.
    """

    def _process_pipeline(self, read_stream):
        duration_stream = read_stream \
            .where("started_script == '/apps/ThinkAnalytics/ContentIngest/bin/ingest.sh'") \
            .aggregate(Avg(group_fields=["hostname"], aggregation_field="duration",
                           aggregation_name=self._component_name + ".ingest"))

        return [duration_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("started_script", StringType()),
            StructField("message", StringType()),
            StructField("finished_script", StringType()),
            StructField("finished_time", TimestampType()),
            StructField("duration", StringType()),
            StructField("hostname", StringType()),
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ThinkAnalyticsReIngest(configuration, ThinkAnalyticsReIngest.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
