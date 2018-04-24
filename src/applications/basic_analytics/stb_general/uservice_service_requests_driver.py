"""Module for counting all general analytics metrics for EOS STB component"""
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_regex
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class USerserviceServiceRequest(BasicAnalyticsProcessor):

    def _process_pipeline(self, read_stream):
        stream = read_stream \
            .where(col("header").getItem("x-dev").isNotNull()) \
            .withColumn("tenant", regexp_extract(col("stack"), "-(\w+)$", 1))

        count_by_app = stream.aggregate(Count(group_fields=["tenant", "app"], aggregation_name=self._component_name))

        count_by_app_status = stream \
            .withColumn("status", custom_translate_regex(
                source_field=col("status"),
                mapping={"^2\d\d": "successful"},
                default_value="failure")) \
            .aggregate(Count(group_fields=["tenant", "app", "status"], aggregation_name=self._component_name))

        return [count_by_app, count_by_app_status]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("app", StringType()),
            StructField("stack", StringType()),
            StructField("status", StringType()),
            StructField("header", StructType([
                StructField("x-dev", StringType()),
            ]))
        ])


def create_processor(configuration):
    return USerserviceServiceRequest(configuration, USerserviceServiceRequest.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
