"""
The module for the driver to calculate metrics related to Think Analytics HTTP access component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UsageCollectorDroppedEvents(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Think Analytics HTTP access component.
    """

    def _process_pipeline(self, read_stream):

        count_missed_events = read_stream \
            .where("UsageCollectorReport_missed_events is not null") \
            .aggregate(Count(group_fields=["UsageCollectorReport_missed_events"], aggregation_name=self._component_name + ".count"))

        return [count_missed_events]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("UsageCollectorReport_missed_events", IntegerType()),
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return UsageCollectorDroppedEvents(configuration, UsageCollectorDroppedEvents.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
