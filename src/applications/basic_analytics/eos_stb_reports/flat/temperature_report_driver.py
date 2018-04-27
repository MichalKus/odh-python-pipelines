"""
Module for counting all general analytics metrics for EOS STB Temperature Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType, \
    FloatType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg
from pyspark.sql.functions import col, from_unixtime


class TemperatureReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for Temperature Reports
    """

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp",
                                      from_unixtime(col("TemperatureReport.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):
        self._common_wifi_pipeline = read_stream \
            .select("@timestamp",
                    "TemperatureReport.*")

        return [self.average_temperature()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("TemperatureReport", StructType([
                StructField("ts", LongType()),
                StructField("name", StringType()),
                StructField("value", FloatType()),
            ]))
        ])

    def average_temperature(self):
        return self._common_wifi_pipeline \
            .where(col("value") >= 0) \
            .aggregate(Avg(aggregation_field="value", group_fields=["name"],
                           aggregation_name=self._component_name + ".degrees"))


def create_processor(configuration):
    """
    Method to create the instance of the processor
    """
    return TemperatureReportEventProcessor(configuration, TemperatureReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
