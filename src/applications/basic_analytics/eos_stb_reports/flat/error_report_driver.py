"""
Module for counting all general analytics metrics for EOS STB ErrorReport Report
"""
from pyspark.sql.types import StructField, StructType, StringType

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

        return [self.__replay_errors(error_report_stream),
                self.__rec_playout_errors(error_report_stream),
                self.__ltv_playout_errors(error_report_stream),
                self.__rev_buffer_playouts(error_report_stream),
                self.__search_failures(error_report_stream),
                self.__error_all_code(error_report_stream),
                self.__count_player_live_tv_errors(error_report_stream),
                self.__count_player_review_buffer_errors(error_report_stream),
                self.__count_player_vod_playback_errors(error_report_stream),
                self.__count_ott_playback_errors(error_report_stream),
                self.__count_content_on_demand_purchase_errors(error_report_stream),
                self.__count_pvr_errors(error_report_stream),
                self.__count_search_errors(error_report_stream),
                self.__count_profile_errors(error_report_stream),
                self.__count_player_recording_errors(error_report_stream),
                self.__count_replay_playback_errors(error_report_stream),
                self.__count_content_on_demand_errors(error_report_stream),
                self.__count_network_errors(error_report_stream),
                self.__count_npvr_errors(error_report_stream),
                self.__count_local_hard_disc_errors(error_report_stream),
                self.__count_software_errors(error_report_stream)]

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
            .aggregate(Count(group_fields=["code"],
                             aggregation_field="viewer_id",
                             aggregation_name=self._component_name))

    def __count_player_live_tv_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code").between(2000, 2002)) |
                   (col("code") == 2004) |
                   (col("code") == 2006) |
                   (col("code") == 2010) |
                   (col("code") == 2020) |
                   (col("code") == 2050)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".player_live_tv_errors"))

    def __count_player_review_buffer_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 2100) |
                   (col("code") == 2105) |
                   (col("code") == 2107) |
                   (col("code") == 2111) |
                   (col("code") == 2112) |
                   (col("code") == 2114) |
                   (col("code") == 2117) |
                   (col("code") == 2118) |
                   (col("code") == 2120) |
                   (col("code") == 2130)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".player_review_buffer_errors"))

    def __count_player_vod_playback_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 2305) |
                   (col("code") == 2307) |
                   (col("code") == 2310) |
                   (col("code") == 2311) |
                   (col("code") == 2312) |
                   (col("code") == 2314) |
                   (col("code") == 2317) |
                   (col("code") == 2318)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".player_vod_playback_errors"))

    def __count_ott_playback_errors(self, error_report_stream):
        return error_report_stream \
            .where(col("code") == 2400) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".ott_playback_errors"))

    def __count_content_on_demand_purchase_errors(self, error_report_stream):
        return error_report_stream \
            .where(col("code").between(4200, 4299)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".content_on_demand_purchase_errors"))

    def __count_pvr_errors(self, error_report_stream):
        return error_report_stream \
            .where(col("code").between(5000, 5099)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".pvr_errors"))

    def __count_search_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code").between(8400, 8499)) |
                   (col("code") == 8200)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".search_errors"))

    def __count_profile_errors(self, error_report_stream):
        return error_report_stream \
            .where(col("code") == 6100) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".profile_errors"))

    def __count_player_recording_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 2200) |
                   (col("code") == 2205) |
                   (col("code") == 2207) |
                   (col("code") == 2211) |
                   (col("code") == 2212) |
                   (col("code") == 2214) |
                   (col("code") == 2217) |
                   (col("code") == 2218)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".player_recording_errors"))

    def __count_replay_playback_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 2505) |
                   (col("code") == 2507) |
                   (col("code") == 2510) |
                   (col("code") == 2511) |
                   (col("code") == 2512) |
                   (col("code") == 2514) |
                   (col("code") == 2517) |
                   (col("code") == 2518)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".replay_playback_errors"))

    def __count_content_on_demand_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 3200) |
                   (col("code") == 3300) |
                   (col("code") == 3400)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".content_on_demand_errors"))

    def __count_network_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code").between(9003, 9006)) |
                   (col("code").between(9993, 9996)) |
                   (col("code") == 9031)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".network_errors"))

    def __count_npvr_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code").between(5600, 5699)) |
                   (col("code") == 5500)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".npvr_errors"))

    def __count_local_hard_disc_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code") == 6000) |
                   (col("code") == 6030)) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".local_hard_disc_errors"))

    def __count_software_errors(self, error_report_stream):
        return error_report_stream \
            .where((col("code").between(7503, 7504)) |
                   (col("code").between(7506, 7508))) \
            .aggregate(Count(group_fields=["code"],
                             aggregation_name=self._component_name + ".software_errors"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return ErrorReportEventProcessor(configuration, ErrorReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
