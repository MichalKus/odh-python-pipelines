"""
Module for counting all general analytics metrics for EOS STB component
"""
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, LongType

from common.basic_analytics.aggregations import DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class StbUsageCollectorReportProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to STB Usage Collector Report component.
    """

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("UsageCollectorReport.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, stream):
        usage_stream = stream \
            .filter(col("UsageCollectorReport.retries") >= 1) \
            .withColumn("viewerID", col("header").getItem("viewerID")) \
            .aggregate(DistinctCount(aggregation_field="viewerID",
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
    return StbUsageCollectorReportProcessor(configuration, StbUsageCollectorReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
