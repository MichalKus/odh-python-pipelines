"""
Basic analytics driver for STB Usage Collector Report
"""
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from common.basic_analytics.aggregations import DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class StbUsageCollectorReportProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to STB Usage Collector Report component.
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "UsageCollectorReport.ts", "@timestamp")

    def _process_pipeline(self, stream):
        usage_stream = stream \
            .select("@timestamp", "UsageCollectorReport.*", col("header.viewerID").alias("viewer_id")) \
            .filter(col("UsageCollectorReport.retries") >= 1) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".with_retries"))
        return usage_stream

    @staticmethod
    def create_schema():
        return StructType([
            StructField("UsageCollectorReport", StructType([
                StructField("retries", IntegerType()),
                StructField("ts", LongType())])),
            StructField("header", StructType([
                StructField("viewerID", StringType())])),
        ])


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return StbUsageCollectorReportProcessor(configuration, StbUsageCollectorReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
