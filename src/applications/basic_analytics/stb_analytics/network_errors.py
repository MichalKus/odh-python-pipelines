"""
Basic analytics driver for STB Network and Connectivity errors.
"""
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType
from common.basic_analytics.aggregations import Count, Sum, Max, Min
from pyspark.sql.functions import col


class NetworkErrorsStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    Basic analytics driver for STB Network and Connectivity errors.
    """

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _process_pipeline(self, json_stream):
        stream = json_stream\
            .withColumn("Ethernet_TxErrors", col("EthernetStats_txErrors").cast(IntegerType()))\
            .withColumn("Ethernet_RxErrors", col("EthernetStats_rxErrors").cast(IntegerType()))\
            .withColumn("Wifi_TxErrors", col("WiFiStats_txErrors").cast(IntegerType()))\
            .withColumn("Wifi_RxErrors", col("WiFiStats_rxErrors").cast(IntegerType()))

        error_report_dimensions = self.__dimensions[:].append("ErrorReport_level")
        aggregations = [stream.aggregate(Count(
            group_fields=error_report_dimensions,
            aggregation_name=self._component_name,
            aggregation_field="ErrorReport_level"))
            ]

        aggregation_fields = ["Ethernet_TxErrors", "Ethernet_RxErrors", "Wifi_TxErrors", "Wifi_RxErrors"]

        for field in aggregation_fields:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field}
            aggregations += [
                stream.aggregate(Sum(**kwargs)),
                stream.aggregate(Count(**kwargs)),
                stream.aggregate(Max(**kwargs)),
                stream.aggregate(Min(**kwargs))
            ]

        return aggregations

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("hardwareVersion", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("EthernetStats_txErrors", StringType()),
            StructField("EthernetStats_rxErrors", StringType()),
            StructField("WiFiStats_txErrors", StringType()),
            StructField("WiFiStats_rxErrors", StringType()),
            StructField("ErrorReport_level", StringType()),
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return NetworkErrorsStbBasicAnalytics(configuration, NetworkErrorsStbBasicAnalytics.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
