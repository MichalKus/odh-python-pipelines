"""
Module for counting all general analytics metrics for EOS STB Screen Load Report
"""
from pyspark.sql.types import StructField, StructType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount, Count
from pyspark.sql.functions import col


class ScreenLoadReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for Screen Load Reports
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "ScreenLoadReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):
        common_screen_load_pipeline = read_stream \
            .select("@timestamp",
                    col("ScreenLoadReport.id").alias("loading_screen"),
                    col("header.viewerID").alias("viewer_id"))

        return [self.__total_app_events(common_screen_load_pipeline),
                self.__viewer_id_distinct_count_per_loading_screens(common_screen_load_pipeline)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("ScreenLoadReport", StructType([
                StructField("ts", LongType()),
                StructField("id", StringType())
            ]))
        ])

    def __total_app_events(self, common_screen_load_pipeline):
        return common_screen_load_pipeline \
            .where("loading_screen is not NULL") \
            .aggregate(Count(group_fields=["loading_screen"], aggregation_field="viewer_id",
                             aggregation_name=self._component_name))

    def __viewer_id_distinct_count_per_loading_screens(self, common_screen_load_pipeline):
        return common_screen_load_pipeline \
            .where("loading_screen is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["loading_screen"],
                                     aggregation_name=self._component_name,
                                     aggregation_window=self._get_interval_duration("uniqCountWindow")))


def create_processor(configuration):
    """
    Method to create the instance of the Screen Load Report processor
    """
    return ScreenLoadReportEventProcessor(configuration, ScreenLoadReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
