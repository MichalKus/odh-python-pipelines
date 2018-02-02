"""
The module for the driver to calculate metrics related to Think Analytics HTTP access component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType
from pyspark.sql.functions import *

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg, Sum, Min, Max
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UsageCollectorDroppedEvents(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to UsageCollector DroppedEvents component.
    """

    def _process_pipeline(self, read_stream):

        count_missed_events = read_stream   \
            .withColumn("UsageCollectorReport_missed_events", col("UsageCollectorReport_missed_events").cast(LongType())) \
            .where("UsageCollectorReport_missed_events is not null") \
            .aggregate(Count(   aggregation_field="UsageCollectorReport_missed_events",
                                group_fields=["hardwareVersion","firmwareVersion","asVersion","appVersion"],
                                aggregation_name=self._component_name))

        sum_missed_events = read_stream \
            .withColumn("UsageCollectorReport_missed_events", col("UsageCollectorReport_missed_events").cast(LongType())) \
            .where("UsageCollectorReport_missed_events is not null") \
            .aggregate(Sum( aggregation_field="UsageCollectorReport_missed_events",
                            group_fields=["hardwareVersion","firmwareVersion","asVersion","appVersion"],
                            aggregation_name=self._component_name))

        min_missed_events = read_stream \
            .withColumn("UsageCollectorReport_missed_events", col("UsageCollectorReport_missed_events").cast(LongType())) \
            .where("UsageCollectorReport_missed_events is not null") \
            .aggregate(Min( aggregation_field="UsageCollectorReport_missed_events",
                            group_fields=["hardwareVersion","firmwareVersion","asVersion","appVersion"],
                            aggregation_name=self._component_name))

        max_missed_events = read_stream \
            .withColumn("UsageCollectorReport_missed_events", col("UsageCollectorReport_missed_events").cast(LongType())) \
            .where("UsageCollectorReport_missed_events is not null") \
            .aggregate(Max( aggregation_field="UsageCollectorReport_missed_events",
                            group_fields=["hardwareVersion","firmwareVersion","asVersion","appVersion"],
                            aggregation_name=self._component_name))


        return [count_missed_events, sum_missed_events, min_missed_events, max_missed_events]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("UsageCollectorReport_missed_events", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("asVersion", StringType()),
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return UsageCollectorDroppedEvents(configuration, UsageCollectorDroppedEvents.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
