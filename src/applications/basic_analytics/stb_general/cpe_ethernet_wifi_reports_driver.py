"""
Module for counting all general analytics metrics for EOS STB Ethernet/Wifi Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, ArrayType, IntegerType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount, Avg
from pyspark.sql.functions import col, explode, from_unixtime


class EthernetWifiReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for Ethernet/Wifi Reports
    """

    def _process_pipeline(self, read_stream):

        self._common_wifi_pipeline = read_stream \
            .select("WiFiStats.*",
                    col("header.viewerID").alias("viewer_id")) \
            .withColumn("@timestamp", from_unixtime(col("ts") / 1000).cast(TimestampType()))

        self._common_ethernet_pipeline = read_stream \
            .select("EthernetStats.*",
                    col("header.viewerID").alias("viewer_id")) \
            .withColumn("@timestamp", from_unixtime(col("ts") / 1000).cast(TimestampType()))

        self._common_net_configuration_pipeline = read_stream \
            .select(explode("NetConfiguration.ifaces").alias("ifaces"),
                    "NetConfiguration.ts",
                    col("header.viewerID").alias("viewer_id")) \
            .withColumn("@timestamp", from_unixtime(col("ts") / 1000).cast(TimestampType())) \
            .select("@timestamp",
                    col("ifaces.enabled").alias("net_config_enabled"),
                    col("ifaces.type").alias("net_configuration_type"),
                    col("viewer_id"))

        return [self.distinct_total_wifi_network_types_count(),
                self.distinct_total_ethernet_network_types_count(),
                self.ethernet_average_upstream_kbps(),
                self.ethernet_average_downstream_kbps(),
                self.wireless_average_upstream_kbps(),
                self.wireless_average_downstream_kbps(),
                self.distinct_total_net_config_enabled(),
                self.total_cpe_net_config_for_wifi_ethernet_channels()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("WiFiStats", StructType([
                StructField("ts", LongType()),
                StructField("type", StringType()),
                StructField("rxKbps", IntegerType()),
                StructField("txKbps", IntegerType()),
                StructField("RSSi", ArrayType(IntegerType()))
            ])),
            StructField("EthernetStats", StructType([
                StructField("ts", LongType()),
                StructField("type", StringType()),
                StructField("rxKbps", IntegerType()),
                StructField("txKbps", IntegerType())
            ])),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("NetConfiguration", StructType([
                StructField("ts", LongType()),
                StructField("ifaces", ArrayType(
                    StructType([
                        StructField("enabled", StringType()),
                        StructField("type", StringType())
                    ]),
                ))
            ]))
        ])

    def distinct_total_wifi_network_types_count(self):
        return self._common_wifi_pipeline \
            .where((col("rxKbps") >= 1) | (col("txKbps") >= 1)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".wifi_network"))

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

    def wireless_average_upstream_kbps(self):
        return self._common_wifi_pipeline \
            .where("txKbps is not NULL") \
            .aggregate(Avg(aggregation_field="txKbps",
                           aggregation_name=self._component_name + ".wireless.average_upstream_kbps"))

    def wireless_average_downstream_kbps(self):
        return self._common_wifi_pipeline \
            .where("rxKbps is not NULL") \
            .aggregate(Avg(aggregation_field="rxKbps",
                           aggregation_name=self._component_name + ".wireless.average_downstream_kbps"))

    def distinct_total_net_config_enabled(self):
        return self._common_net_configuration_pipeline \
            .where("net_config_enabled is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["net_config_enabled"],
                                     aggregation_name=self._component_name+".stb_with"))

    def total_cpe_net_config_for_wifi_ethernet_channels(self):
        return self._common_net_configuration_pipeline \
            .where("net_configuration_type is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["net_configuration_type"],
                                     aggregation_name=self._component_name+".total_cpe_net_config"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return EthernetWifiReportEventProcessor(configuration, EthernetWifiReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
