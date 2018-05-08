"""
Module for counting all general analytics metrics for EOS STB BCMReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, BooleanType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col


class BCMReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for BCMReport Reports
    """
    def _prepare_timefield(self, read_stream):
        return convert_epoch_to_iso(read_stream, "BCMReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        bcm_report_stream = read_stream \
            .select("@timestamp", "BCMReport.*", col("header.viewerID").alias("viewer_id"))

        return [self.distinct_active_stb_4k_enabled(bcm_report_stream),
                self.distinct_active_stb_4k_disabled(bcm_report_stream)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("BCMReport", StructType([
                StructField("ts", LongType()),
                StructField("4Kcontent", BooleanType())
            ]))
        ])

    def distinct_active_stb_4k_enabled(self, read_stream):
        return read_stream \
            .where("4Kcontent = true") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_enabled"))

    def distinct_active_stb_4k_disabled(self, read_stream):
        return read_stream \
            .where("4Kcontent = false") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_disabled"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return BCMReportEventProcessor(configuration, BCMReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
