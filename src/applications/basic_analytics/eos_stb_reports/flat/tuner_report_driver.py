"""
Module for counting all general analytics metrics for EOS STB TunerReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, BooleanType, LongType, FloatType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg, DistinctCount
from pyspark.sql.functions import col


class TunerReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for TunerReport Reports
    """
    def _prepare_timefield(self, read_stream):
        return convert_epoch_to_iso(read_stream, "TunerReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        tuner_report_stream = read_stream \
            .select("@timestamp", "TunerReport.*", col("header.viewerID").alias("viewer_id"))

        return [self.__avg_snr(tuner_report_stream),
                self.__avg_signal_level_dbm(tuner_report_stream),
                self.__distinct_stb_by_report_index(tuner_report_stream),
                self.__avg_frequency_stb_by_report_index(tuner_report_stream)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("TunerReport", StructType([
                StructField("ts", LongType()),
                StructField("index", StringType()),
                StructField("signalLevel", FloatType()),
                StructField("SNR", StringType()),
                StructField("locked", BooleanType()),
                StructField("frequency", LongType())
            ]))
        ])

    def __avg_snr(self, read_stream):
        return read_stream \
            .where("SNR is not NULL") \
            .aggregate(Avg(aggregation_field="SNR",
                           aggregation_name=self._component_name))

    def __avg_signal_level_dbm(self, read_stream):
        return read_stream \
            .where("signalLevel is not NULL") \
            .aggregate(Avg(aggregation_field="signalLevel",
                           aggregation_name=self._component_name + ".dbm"))

    def __distinct_stb_by_report_index(self, read_stream):
        return read_stream \
            .where("locked = true") \
            .aggregate(DistinctCount(group_fields=["index"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".locked"))

    def __avg_frequency_stb_by_report_index(self, read_stream):
        return read_stream \
            .where("locked = true") \
            .where("frequency is not NULL") \
            .aggregate(Avg(group_fields=["index"],
                           aggregation_field="frequency",
                           aggregation_name=self._component_name + ".locked"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return TunerReportEventProcessor(configuration, TunerReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
