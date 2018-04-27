"""
Module for counting all general analytics metrics for EOS STB BCMReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, BooleanType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, from_unixtime


class BCMReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for BCMReport Reports
    """
    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("BCMReport.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):

        self._bcm_report_stream = read_stream \
            .select("@timestamp",
                    "BCMReport.*",
                    col("header.viewerID").alias("viewer_id"))

        return [self.distinct_active_stb_4k_enabled(),
                self.distinct_active_stb_4k_disabled()]

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

    def distinct_active_stb_4k_enabled(self):
        return self._bcm_report_stream \
            .where("4Kcontent = true") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_enabled"))

    def distinct_active_stb_4k_disabled(self):
        return self._bcm_report_stream \
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
