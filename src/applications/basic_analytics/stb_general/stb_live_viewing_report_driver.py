"""Module for counting live viewing report analytics metrics for EOS STB component"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import DistinctCount, Count
from pyspark.sql.functions import col, from_unixtime
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class StbLiveViewingReportProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate live viewing report graphite-friendly metrics.
    (aggregations, filtering and other performance-heavy operations performed by spark)
    """

    def _process_pipeline(self, read_stream):

        """
        Extract viewerID from handled record header -> frequently used in metrics.
        """
        read_stream = read_stream \
            .withColumn("viewer_id", col("header").getItem("viewerID")) \

        self._tuner_report_stream = read_stream \
            .withColumn("@timestamp", from_unixtime(col("AMSLiveViewingReport.ts") / 1000).cast(TimestampType())) \
            .withColumn("id", col("AMSLiveViewingReport").getItem("id")) \
            .withColumn("event_type", col("AMSLiveViewingReport").getItem("event_type"))

        return [self.count_distinct_event_type_by_id(),
                self.count_event_type_by_id(),
                self.count_popular_channels_by_id()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType()),
            ])),
            StructField("AMSLiveViewingReport", StructType([
                StructField("ts", LongType()),
                StructField("id", StringType()),
                StructField("event_type", StringType())
            ]))
        ])

    def count_distinct_event_type_by_id(self):
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
    return StbLiveViewingReportProcessor(configuration, StbLiveViewingReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
