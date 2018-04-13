"""
The module for the driver to calculate metrics related to Catalina component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count
from pyspark.sql.functions import col
from common.spark_utils.custom_functions import custom_translate_regex
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class CatalinaConnectionEventsProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Catalina component.
    """
    mapping = {
        r".*connection\saccepted.*": "total_count_of_accepted_network_connections"
    }

    def _process_pipeline(self, read_stream):
        count_by_connections_types = read_stream \
            .filter("event_type == 'NETWORK'") \
            .withColumn(
                "message_type",
                custom_translate_regex(
                    source_field=col("message"),
                    mapping=self.mapping,
                    default_value="unclassified"
                )
            ) \
            .filter("message_type != 'unclassified'") \
            .aggregate(Count(group_fields=["event_type", "message_type"], aggregation_name=self._component_name))

        return [count_by_connections_types]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("event_type", StringType()),
            StructField("message", StringType()),
            StructField("thread", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return CatalinaConnectionEventsProcessor(configuration, CatalinaConnectionEventsProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
