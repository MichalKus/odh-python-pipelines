"""
Module for counting all general analytics metrics for EOS STB AMSLiveViewingReport Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, BooleanType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Count, DistinctCount
from pyspark.sql.functions import col, from_unixtime


class AMSLiveViewingReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for AMSLiveViewingReport Reports
    """
    def _prepare_timefield(self, data_stream):
        return data_stream \
            .withColumn("@timestamp", from_unixtime(col("AMSLiveViewingReport.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):

        self._tuner_report_stream = read_stream \
            .select("@timestamp",
                    "AMSLiveViewingReport.*",
                    col("header.viewerID").alias("viewer_id"))

        return [self.distinct_event_type_by_id(),
                self.count_event_type_by_id(),
                self.count_popular_channels_by_id()]

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

    def distinct_event_type_by_id(self):
        return self._tuner_report_stream \
            .where("event_type = 'TUNE_IN'") \
            .aggregate(DistinctCount(group_fields=["id"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".tune_in"))

    def count_event_type_by_id(self):
        return self._tuner_report_stream \
            .where("event_type = 'TUNE_IN'") \
            .aggregate(Count(group_fields=["id"],
                             aggregation_name=self._component_name + ".tune_in"))

    def count_popular_channels_by_id(self):
        return self._tuner_report_stream \
            .aggregate(Count(group_fields=["id"],
                             aggregation_name=self._component_name + ".popular_channels"))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return AMSLiveViewingReportEventProcessor(configuration, AMSLiveViewingReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
