import sys
from pyspark.sql.types import *

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from common.spark_utils.custom_functions import *
from util.utils import Utils


class ProdisWS(BasicAnalyticsProcessor):
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
    return ProdisWS(configuration, ProdisWS.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
