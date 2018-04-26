"""
Basic analytics driver for STB Voice Report values for all hardware components.
"""
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, LongType

from common.basic_analytics.aggregations import Count, Max, Min, CompoundAggregation
from common.basic_analytics.aggregations import P01, P05, P10, P25, P50, P75, P90, P95, P99
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class VoiceReportStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    Basic analytics driver for STB Voice Report values for all hardware components.
    """

    __dimensions = ["viewerID", "sessionId"]

    def _process_pipeline(self, json_stream):

        stream = json_stream \
            .filter(col("VoiceReport.voiceReport.sessionId").isNotNull()) \
            .select(
                col("@timestamp"),
                col("header.viewerID").alias("viewerID"),
                col("VoiceReport.voiceReport.sessionId").alias("sessionId"),
                col("VoiceReport.voiceReport.sessionCreationTime").alias("sessionCreationTime"),
                col("VoiceReport.voiceReport.audioPacketLoss").alias("audioPacketLoss"),
                col("VoiceReport.voiceReport.audioTransferTime").alias("audioTransferTime"),
                col("VoiceReport.voiceReport.transactionResult").alias("transactionResult")
            )

        aggregation_fields = ["sessionCreationTime", "audioPacketLoss", "audioTransferTime"]
        aggregations = []

        for field in aggregation_fields:
            kwargs = {'aggregation_field': field}

            aggregations.extend([Count(**kwargs), Max(**kwargs), Min(**kwargs),
                                 P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                                 P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)])

        return [
            stream.aggregate(CompoundAggregation(aggregations=aggregations,
                                                 group_fields=self.__dimensions,
                                                 aggregation_name=self._component_name)),
            stream.aggregate(Count(
                group_fields=["viewerID", "sessionId", "transactionResult"],
                aggregation_name=self._component_name))
        ]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("VoiceReport", StructType([
                StructField("voiceReport", StructType([
                    StructField("ts", LongType()),
                    StructField("sessionId", StringType()),
                    StructField("transactionResult", StringType()),
                    StructField("sessionCreationTime", LongType()),
                    StructField("audioPacketLoss", LongType()),
                    StructField("audioTransferTime", LongType())
                ]))
            ]))
        ])

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "VoiceReport.voiceReport.ts", "@timestamp")

    def _post_processing_pipeline(self, dataframe):
        return dataframe.na.drop()


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return VoiceReportStbBasicAnalytics(configuration, VoiceReportStbBasicAnalytics.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
