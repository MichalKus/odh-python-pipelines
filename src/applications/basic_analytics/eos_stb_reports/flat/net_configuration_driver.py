"""
Module for counting all general analytics metrics for EOS STB NetConfiguration Report
"""
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, explode


class NetConfigurationReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for NetConfiguration Reports
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "NetConfiguration.ts", "@timestamp")

    def _process_pipeline(self, read_stream):
        self._common_net_configuration_pipeline = read_stream \
            .select("@timestamp",
                    explode("NetConfiguration.ifaces").alias("ifaces"),
                    col("header.viewerID").alias("viewer_id")) \
            .select("@timestamp",
                    col("ifaces.enabled").alias("net_config_enabled"),
                    col("ifaces.type").alias("net_configuration_type"),
                    col("viewer_id"))

        return [self.distinct_total_net_config_enabled(),
                self.total_cpe_net_config_for_wifi_ethernet_channels()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("NetConfiguration", StructType([
                StructField("ts", LongType()),
                StructField("ifaces", ArrayType(
                    StructType([
                        StructField("enabled", StringType()),
                        StructField("type", StringType())
                    ])
                ))
            ]))
        ])

    def distinct_total_net_config_enabled(self):
        return self._common_net_configuration_pipeline \
            .where("net_config_enabled is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["net_config_enabled"],
                                     aggregation_name=self._component_name))

    def total_cpe_net_config_for_wifi_ethernet_channels(self):
        return self._common_net_configuration_pipeline \
            .where("net_configuration_type is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["net_configuration_type"],
                                     aggregation_name=self._component_name+".total_cpe_net_config"))


def create_processor(configuration):
    """
    Method to create the instance of the Net Configuration processor
    """
    return NetConfigurationReportEventProcessor(configuration, NetConfigurationReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)