from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import *
from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UsageCollectorEventTypeReportProcessor(BasicAnalyticsProcessor):
    """
    EOS STB driver contains logic for calculation counts of UsageCollectorReport Event Types:
    - count
    """

    def _process_pipeline(self, read_stream):
        group_by_list = ["hardwareVersion", "firmwareVersion", "asVersion", "appVersion",
                         "UsageCollectorReport_event_Type"]

        aggregation = Count(group_fields=group_by_list, aggregation_name=self._component_name)

        start_count = read_stream \
            .filter(col("UsageCollectorReport_event_Type") == "start") \
            .aggregate(aggregation)

        callback_failed_count = read_stream \
            .filter(col("UsageCollectorReport_event_Type") == "callback_failed") \
            .aggregate(aggregation)

        memory_full_count = read_stream \
            .filter(col("UsageCollectorReport_event_Type") == "memory_full") \
            .aggregate(aggregation)

        degraded_mode_count = read_stream \
            .filter(col("UsageCollectorReport_event_Type") == "degraded_mode") \
            .aggregate(aggregation)


        return [start_count, callback_failed_count, memory_full_count, degraded_mode_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("UsageCollectorReport_event_type", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("appVersion", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return UsageCollectorEventTypeReportProcessor(configuration,
                                                  UsageCollectorEventTypeReportProcessor.create_schema(),
                                                  )


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
