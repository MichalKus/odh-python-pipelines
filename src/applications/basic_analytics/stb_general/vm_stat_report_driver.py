"""Module for counting all general analytics metrics for EOS STB Ethernet/Wifi Report"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, ArrayType, IntegerType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount, Avg
from pyspark.sql.functions import col, explode


class VmStatReportEventProcessor(BasicAnalyticsProcessor):
    """Class that's responsible to process pipelines for Ethernet/Wifi Reports"""

    def _process_pipeline(self, read_stream):
        report_generator = EthernetWifiReports(read_stream, self._component_name)

        return []

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("WiFiStats", StructType([
                StructField("type", StringType()),
                StructField("rxKbps", IntegerType()),
                StructField("txKbps", IntegerType()),
                StructField("RSSi", ArrayType(IntegerType()))
            ])),
            StructField("EthernetStats", StructType([
                StructField("type", StringType()),
                StructField("rxKbps", IntegerType()),
                StructField("txKbps", IntegerType())
            ])),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
            StructField("NetConfiguration", StructType([
                StructField("ifaces", ArrayType(
                    StructType([
                        StructField("enabled", StringType()),
                        StructField("type", StringType())
                    ]),
                ))
            ]))
        ])


class EthernetWifiReports(object):
    """Class that is able to create output streams with metrics for Ethernet Wifi Reports"""

    def __init__(self, read_stream, component_name):
        self._component_name = component_name

        # Common Wifi Report
        self._common_vm_stat_pipeline = read_stream \
            .select("@timestamp",
                    "WiFiStats.*",
                    col("header.viewerID").alias("viewer_id"))

    # STB Network Type
    def distinct_total_wifi_network_types_count(self):
        return self._common_vm_stat_pipeline \
            .where((col("rxKbps") >= 1) | (col("txKbps") >= 1)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["type"],
                                     aggregation_name=self._component_name + ".network_type"))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return VmStatReportEventProcessor(configuration, VmStatReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
