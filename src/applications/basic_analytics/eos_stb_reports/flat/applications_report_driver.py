"""
Module for counting all general analytics metrics for EOS STB ApplicationsReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, from_unixtime


class ApplicationsReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for ApplicationsReport Reports
    """
    def _prepare_timefield(self, data_stream):
        return data_stream \
            .withColumn("@timestamp", from_unixtime(col("ApplicationsReport.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):

        self._applications_report_stream = read_stream \
            .select("@timestamp",
                    "ApplicationsReport.*",
                    col("header.viewerID").alias("viewer_id"))

        return [self.distinct_active_stb_netflix(),
                self.distinct_active_stb_youtube()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("ApplicationsReport", StructType([
                StructField("ts", LongType()),
                StructField("provider_id", StringType())
            ]))
        ])

    def distinct_active_stb_netflix(self):
        return self._applications_report_stream \
            .where("provider_id = 'netflix'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".netflix"))

    def distinct_active_stb_youtube(self):
        return self._applications_report_stream \
            .where("provider_id = 'youtube'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".youtube"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return ApplicationsReportEventProcessor(configuration, ApplicationsReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
