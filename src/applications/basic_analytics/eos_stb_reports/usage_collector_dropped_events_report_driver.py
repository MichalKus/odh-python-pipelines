"""
The module for the driver to calculate metrics related to UsageCollector for dropped events.
"""
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Sum, Count, Max, Min, Stddev, P01, P05, P10, P25, P50, \
    P75, P90, P95, P99, CompoundAggregation
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.spark_utils.custom_functions import convert_epoch_to_iso


class UsageCollectorDroppedEvents(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to UsageCollector DroppedEvents component.
    """
    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    def _process_pipeline(self, json_stream):
        stream = json_stream .withColumn("UsageCollectorReport_missed_events",
                        col("UsageCollectorReport_missed_events").cast(IntegerType()))

        kwargs = {"aggregation_field": "UsageCollectorReport_missed_events"}

        aggregations = [Sum(**kwargs), Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                        P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                        P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

        return [stream.aggregate(CompoundAggregation(aggregations=aggregations, group_fields=self.__dimensions,
                                                     aggregation_name=self._component_name))]

    @staticmethod
    def create_schema():
        """
            Returns fields needed for processing.
            UsageCollectorReport_missed_events contains the number of missed events
            Other fields describes the origin of the metrics.
        """
        return StructType([
            StructField("timestamp", StringType()),
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
