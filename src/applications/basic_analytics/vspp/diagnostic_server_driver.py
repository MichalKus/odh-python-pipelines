"""
The module for the driver to calculate metrics related to vspp data.
"""
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count
from common.spark_utils.custom_functions import custom_translate_regex
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class VsppDiagnosticServer(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to vspp data.
    """

    def _process_pipeline(self, read_stream):
        mapping = {
            ".*error got \d*, expected \d*, packet \d*.*":
                "total_count_of_cc_error",
            ".*Ingest session service error, failed starting session handler, nested error:.*":
                "total_count_of_cc_ingest_session_service_error",
            ".*underflow in input ES.*":
                "total_count_of_cc_corrupt_teletext_index_error"
        }

        count_cc_errors_stream = read_stream \
            .withColumn("message_type",
                        custom_translate_regex(
                            source_field=col("message"),
                            mapping=mapping,
                            default_value="unclassified")) \
            .filter("message_type != 'unclassified'") \
            .aggregate(Count(group_fields=["message_type"],
                             aggregation_name=self._component_name))

        return [count_cc_errors_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("message", StringType()),
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return VsppDiagnosticServer(configuration, VsppDiagnosticServer.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
