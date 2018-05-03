"""
Module for counting all general basic analytics metrics for EOS STB Temperature Report
"""
from pyspark.sql.types import StructField, StructType, StringType, LongType, \
    FloatType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg
from pyspark.sql.functions import col


class TemperatureReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for Temperature Reports
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "TemperatureReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):
        self._common_temperature_pipeline = read_stream \
            .select("@timestamp",
                    col("TemperatureReport.name").alias("name"),
                    col("TemperatureReport.value").alias("temperature"))

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
        return self._common_temperature_pipeline \
            .where(col("temperature") >= 0) \
            .aggregate(Avg(aggregation_field="temperature", group_fields=["name"],
                           aggregation_name=self._component_name))


def create_processor(configuration):
    """
    Method to create the instance of the Temperature report processor
    """
    return TemperatureReportEventProcessor(configuration, TemperatureReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
