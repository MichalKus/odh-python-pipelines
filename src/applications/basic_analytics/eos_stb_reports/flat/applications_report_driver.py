"""
Module for counting all general analytics metrics for EOS STB ApplicationsReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col


class ApplicationsReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for ApplicationsReport Reports
    """
    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "ApplicationsReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        self._applications_report_stream = read_stream \
            .select("@timestamp", "ApplicationsReport.*", col("header.viewerID").alias("viewer_id"))

        return [self.distinct_active_stb_netflix(),
                self.distinct_active_stb_youtube(),
                self.distinct_active_stb_all()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("ApplicationsReport", StructType([
                StructField("ts", LongType()),
                StructField("provider_id", StringType()),
                StructField("event_type", StringType())
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

    def distinct_active_stb_all(self):
        return self._applications_report_stream \
            .where("event_type = 'app_started'") \
            .aggregate(DistinctCount(group_fields=["provider_id"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".all"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return ApplicationsReportEventProcessor(configuration, ApplicationsReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
