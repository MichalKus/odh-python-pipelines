"""
The module for the driver to calculate metrics related to Airflow Worker component.
"""
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_regex
from common.basic_analytics.aggregations import Count
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class AirflowWorker(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Airflow Worker component.
    """

    def _process_pipeline(self, read_stream):
        exception_types_stream = read_stream \
            .withColumn("exception_text", regexp_extract("message", r"\nException\:(.+?)\n", 1)) \
            .where("exception_text != ''") \
            .withColumn("exception_type",
                        custom_translate_regex(
                            source_field=col("exception_text"),
                            mapping={
                                ".*Failed to connect to.*": "connection_failed",
                                ".*Bad checksum for the.*": "bad_checksum",
                                ".*packages moved to failed folder.*": "movement_to_failed_folder",
                                ".*managed folder doesn't exist.*": "folder_does_not_exist"
                            },
                            default_value="unclassified")) \
            .aggregate(Count(group_fields=["hostname", "exception_type"], aggregation_name=self._component_name))
        return [exception_types_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("script", StringType()),
            StructField("level", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""

    return AirflowWorker(configuration, AirflowWorker.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
