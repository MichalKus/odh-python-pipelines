"""
Module for counting all general analytics metrics for EOS STB AMSLiveViewingReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Count, DistinctCount
from pyspark.sql.functions import col


class AMSLiveViewingReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for AMSLiveViewingReport Reports
    """
    def _prepare_timefield(self, read_stream):
        return convert_epoch_to_iso(read_stream, "AMSLiveViewingReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        ams_live_viewing_report_stream = read_stream \
            .select("@timestamp", "AMSLiveViewingReport.*", col("header.viewerID").alias("viewer_id")) \
            .withColumn("channel", col("id"))

        return [self.distinct_event_type_by_channel(ams_live_viewing_report_stream),
                self.count_event_type_by_channel(ams_live_viewing_report_stream),
                self.count_popular_channels_by_channel(ams_live_viewing_report_stream)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("AMSLiveViewingReport", StructType([
                StructField("ts", LongType()),
                StructField("id", StringType()),
                StructField("event_type", StringType())
            ]))
        ])

    def distinct_event_type_by_channel(self, read_stream):
        return read_stream \
            .where("event_type = 'TUNE_IN'") \
            .aggregate(DistinctCount(group_fields=["channel"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".tune_in"))

    def count_event_type_by_channel(self, read_stream):
        return read_stream \
            .where("event_type = 'TUNE_IN'") \
            .aggregate(Count(group_fields=["channel"],
                             aggregation_name=self._component_name + ".tune_in"))

    def count_popular_channels_by_channel(self, read_stream):
        return read_stream \
            .aggregate(Count(group_fields=["channel"],
                             aggregation_name=self._component_name + ".popular_channels"))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return AMSLiveViewingReportEventProcessor(configuration, AMSLiveViewingReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
