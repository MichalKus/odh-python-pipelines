"""
The module for the driver to calculate metrics related to Prodis WS component.
"""

from pyspark.sql.types import TimestampType, StructType, StructField, StringType
from pyspark.sql.functions import col, regexp_replace, lower, when

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_regex
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class ProdisWS(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Prodis WS component.
    """

    def __init__(self, configuration, schema):
        self.__component_name = configuration.property("analytics.componentName")
        super(ProdisWS, self).__init__(configuration, schema)

    def _process_pipeline(self, source_stream):
        mapping_thread_name_to_group = \
            {
                "ingest thread": "ingest",
                "asset propagation thread": "asset",
                "product propagation thread": "product",
                "series propagation thread": "product",
                "title propagation thread": "title",
                "hosted thread": "hosted",
                "scheduled task thread": "scheduled",
            }
        prepared_stream = source_stream \
            .withColumn("group",
                        custom_translate_regex(source_field=lower(col("thread_name")),
                                               mapping=mapping_thread_name_to_group,
                                               default_value="unclassified")) \
            .withColumn("level_updated",
                        when(
                            (col("level") == "INFO") &
                            (
                                (col("message").contains("successfully initialized port"))
                                |
                                ~(col("message").rlike("^[^']*(['][^']*?['])*[^']*succe.*"))
                            ),
                            "SUCCESS")
                        .otherwise(col("level"))) \
            .withColumn("level", col("level_updated")) \
            .drop("level_updated") \
            .withColumn("instance_name_updated",
                        when(col("instance_name") == "", "undefined") \
                        .otherwise(regexp_replace("instance_name", "Cleanpu", "Cleanup"))
                        ) \
            .withColumn("instance_name", col("instance_name_updated")) \
            .drop("instance_name_updated") \
            .na.fill("instance_name", "undefined") \
            .where(col("level").isin("SUCCESS", "ERROR", "WARN", "FATAL"))

        counts = prepared_stream.aggregate(
            Count(group_fields=["hostname", "group", "instance_name", "level"], aggregation_name=self.__component_name))
        return [counts]

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
