"""
Module for counting all general analytics metrics for EOS STB Ethernet Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount, Avg
from pyspark.sql.functions import col


class EthernetReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for Ethernet Reports
    """
    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "EthernetStats.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        self._ethernet_stream = read_stream \
            .select("@timestamp", "EthernetStats.*", col("header.viewerID").alias("viewer_id"))

        return [self.distinct_active_stb_ethernet(),
                self.distinct_total_ethernet_network_types_count(),
                self.ethernet_average_upstream_kbps(),
                self.ethernet_average_downstream_kbps()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("EthernetStats", StructType([
                StructField("ts", LongType()),
                StructField("type", StringType()),
                StructField("rxKbps", IntegerType()),
                StructField("txKbps", IntegerType())
            ]))
        ])

    def distinct_active_stb_ethernet(self):
        return self._ethernet_stream \
            .where(col("rxKbps") > 0) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".ethernet_active"))

    def distinct_total_ethernet_network_types_count(self):
        return self._ethernet_stream \
            .where((col("rxKbps") >= 1) | (col("txKbps") >= 1)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".ethernet_network"))

    def ethernet_average_upstream_kbps(self):
        return self._ethernet_stream \
            .where("txKbps is not NULL") \
            .aggregate(Avg(aggregation_field="txKbps",
                           aggregation_name=self._component_name + ".ethernet.average_upstream_kbps"))

    def ethernet_average_downstream_kbps(self):
        return self._ethernet_stream \
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
