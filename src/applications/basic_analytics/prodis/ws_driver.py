"""
The module for the driver to calculate metrics related to Prodis WS component.
"""

from pyspark.sql.functions import col, lower, when
from pyspark.sql.types import TimestampType, StructType, StructField, StringType

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_like
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ProdisWS(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Prodis WS component.
    """

    __mapping_thread_name_to_group = [
        (["asset propagation thread"], "asset propagation"),
        (["product propagation thread"], "product propagation"),
        (["series propagation thread"], "series propagation"),
        (["service propagation thread"], "service propagation"),
        (["scheduled task thread"], "schedule information"),
        (["title propagation thread"], "title propagation"),
        (["hosted thread"], "hosted information"),
        (["ingest thread"], "ingest information"),
        (["image propagation thread"], "image propagation"),
        (["watch dog thread"], "watchdog information"),
        (["change detection thread"], "change detection information"),
        (["main thread"], "main or global information"),
        (["global propagation thread"], "main or global information"),
    ]

    def _process_pipeline(self, source_stream):
        filtered_stream = source_stream \
            .where(col("level") != "DEBUG") \
            .withColumn("level_updated",
                        when((col("level") == "INFO"), "SUCCESS")
                        .when((col("level") == "FATAL"), "ERROR")
                        .otherwise(col("level"))) \
            .withColumn("level", col("level_updated"))

        stream_with_mapped_groups = filtered_stream \
            .withColumn("group", custom_translate_like(source_field=lower(col("thread_name")),
                                                       mappings_pair=self.__mapping_thread_name_to_group,
                                                       default_value="unclassified"))

        count_by_level = stream_with_mapped_groups \
            .where((col("level") != "SUCCESS") | lower(col("message")).like("%succe%")) \
            .aggregate(
                Count(group_fields=["hostname", "level"], aggregation_name=self._component_name + ".prodis_operations"))

        total_count = stream_with_mapped_groups.aggregate(
            Count(group_fields=["hostname"], aggregation_name=self._component_name + ".prodis_operations"))

        total_count_by_group = stream_with_mapped_groups.aggregate(
            Count(group_fields=["hostname", "group"], aggregation_name=self._component_name))

        count_by_group_and_level = stream_with_mapped_groups.aggregate(
            Count(group_fields=["hostname", "group", "level"], aggregation_name=self._component_name))

        return [total_count, count_by_level, total_count_by_group, count_by_group_and_level]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("thread_name", StringType()),
            StructField("instance_name", StringType()),
            StructField("component", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return ProdisWS(configuration, ProdisWS.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
