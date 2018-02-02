from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ErrorLevelReportProcessor(BasicAnalyticsProcessor):
    """
    EOS STB driver contains logic for calculation counts of ErrorReport Level Types:
    - count
    """

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("timestamp") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):
        group_by_list = ["hardwareVersion", "firmwareVersion", "asVersion", "appVersion", "ErrorReport_level"]

        aggregation = Count(group_fields=group_by_list, aggregation_name=self._component_name)

        def get_agg_df(current_value):
            return read_stream \
                .filter(col("ErrorReport_level") == current_value) \
                .aggregate(aggregation)

        return map(get_agg_df, ["WARNING", "ERROR", "CRITICAL"])

    @staticmethod
    def create_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("ErrorReport_level", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ErrorLevelReportProcessor(configuration,
                                     ErrorLevelReportProcessor.create_schema(),
                                     )

if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
