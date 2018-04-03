"""
The module for the driver to calculate metrics related to uServices component.
"""
from collections import namedtuple

from pyspark.sql.functions import col, when, regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, Avg
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline

UServiceInfo = namedtuple("uService", "app api_method")


class UServicesBasycAnalytics(BasicAnalyticsProcessor):
    """
   The processor implementation to calculate metrics related to UServices component.
   """

    def _process_pipeline(self, read_stream):
        # filter useless data
        filtered_stream = read_stream.where(
            (col("duration_ms").cast("long") != 0) &
            ~ (col("requested_url").startswith("GET /info") | col("requested_url").startswith("GET /prometheus"))
        )

        mapped_stream = filtered_stream \
            .withColumn("country",
                        when(col("stack").isNotNull(),
                             regexp_extract("stack", r".*-(\w+)$", 1))
                        .otherwise("undefined"))

        average_duration = mapped_stream.aggregate(
            Avg(group_fields=["country", "host", "app", "app_version", "api_method"],
                aggregation_field="duration_ms",
                aggregation_name=self._component_name))

        count_by_status = mapped_stream.aggregate(
            Count(group_fields=["country", "host", "app", "app_version", "api_method", "status"],
                  aggregation_name=self._component_name))

        return [average_duration, count_by_status]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("host", StringType()),
            StructField("api_method",StringType()),
            StructField("stack", StringType()),
            StructField("app", StringType()),
            StructField("app_version", StringType()),
            StructField("requested_url", StringType()),
            StructField("duration_ms", StringType()),
            StructField("status", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return UServicesBasycAnalytics(configuration, UServicesBasycAnalytics.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
