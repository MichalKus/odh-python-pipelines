"""
Module for counting all general analytics metrics for EOS STB ErrorReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount, Count
from pyspark.sql.functions import col


class ErrorReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for ErrorReport Reports
    """
    def __init__(self, *args, **kwargs):
        super(ErrorReportEventProcessor, self).__init__(*args, **kwargs)
        self._uniq_count_window = self._get_interval_duration("uniqCountWindow")

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "ErrorReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):
        error_report_stream = read_stream \
            .where(col("ErrorReport.code") != "UNKNOWN") \
            .select("@timestamp", "ErrorReport.*", col("header.viewerID").alias("viewer_id"))

        return [self.__replay_errors(error_report_stream), self.__rec_playout_errors(error_report_stream)
            , self.__ltv_playout_errors(error_report_stream), self.__rev_buffer_playouts(error_report_stream)
            , self.__search_failures(error_report_stream), self.__error_all_code(error_report_stream)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("ErrorReport", StructType([
                StructField("ts", StringType()),
                StructField("code", StringType()),
                StructField("ctxt", StructType([
                    StructField("search", StringType())
                ]))
            ]))
        ])

    def __replay_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") >= 2500) & (col("code") <= 2599)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".replay_errors",
                                     aggregation_window=self._uniq_count_window))

    def __rec_playout_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 2217) | (col("code") == 2212)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".rec_playout_errors",
                                     aggregation_window=self._uniq_count_window))

    def __ltv_playout_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 2004) | (col("code") == 2002)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".ltv_playout_errors",
                                     aggregation_window=self._uniq_count_window))

    def __rev_buffer_playouts(self, error_report_stream):
        return error_report_stream \
            .where((col("code") >= 2100) & (col("code") <= 2199)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".rev_buffer_playouts",
                                     aggregation_window=self._uniq_count_window))

    def __search_failures(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 8400) & (col("ctxt.search").isNotNull())) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".search_failures",
                                     aggregation_window=self._uniq_count_window))

    def __error_all_code(self, error_report_stream):
        return error_report_stream \
            .aggregate(Count(group_fields=["code"], aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return ErrorReportEventProcessor(configuration, ErrorReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
