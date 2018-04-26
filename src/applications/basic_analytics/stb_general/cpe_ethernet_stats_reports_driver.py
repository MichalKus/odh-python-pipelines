"""
Module for counting all general analytics metrics for EOS STB Ethernet Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount, Avg
from pyspark.sql.functions import col, from_unixtime


class EthernetReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for Ethernet Reports
    """
    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("EthernetStats.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):

        self._common_ethernet_pipeline = read_stream \
            .select("@timestamp",
                    "EthernetStats.*",
                    col("header.viewerID").alias("viewer_id"))

        return [self.distinct_total_ethernet_network_types_count(),
                self.ethernet_average_upstream_kbps(),
                self.ethernet_average_downstream_kbps()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("EthernetStats", StructType([
                StructField("ts", LongType()),
                StructField("type", StringType()),
                StructField("rxKbps", IntegerType()),
                StructField("txKbps", IntegerType())
            ])),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
        ])

    def distinct_total_ethernet_network_types_count(self):
        return self._common_ethernet_pipeline \
            .where((col("rxKbps") >= 1) | (col("txKbps") >= 1)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".ethernet_network"))

    def ethernet_average_upstream_kbps(self):
        return self._common_ethernet_pipeline \
            .where("txKbps is not NULL") \
            .aggregate(Avg(aggregation_field="txKbps",
                           aggregation_name=self._component_name + ".ethernet.average_upstream_kbps"))

    def ethernet_average_downstream_kbps(self):
        return self._common_ethernet_pipeline \
            .where("rxKbps is not NULL") \
            .aggregate(Avg(aggregation_field="rxKbps",
                           aggregation_name=self._component_name + ".ethernet.average_downstream_kbps"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return EthernetReportEventProcessor(configuration, EthernetReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
