import sys

from pyspark.sql.types import StructType, StructField, TimestampType, StringType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from common.spark_utils.custom_functions import col
from util.utils import Utils


class StbEosProcessor(BasicAnalyticsProcessor):
    """
    Contains logic for calculation several metrics:
    - count
    - unique_count
    """

    def _process_pipeline(self, read_stream):
        stb_ids = read_stream.select(col("xdev").alias("stb_id"), col("time"))
        stb_ids_count = stb_ids.aggregate(
            Count(aggregation_field="stb_id",
                  aggregation_name=self._component_name))

        stb_ids_distinct_count = stb_ids.aggregate(
            DistinctCount(aggregation_field="stb_id",
                          aggregation_name=self._component_name))
        return [stb_ids_count, stb_ids_distinct_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("time", TimestampType()),
            StructField("xdev", StringType()),
        ])

    @staticmethod
    def create_processor(configuration):
        return StbEosProcessor(configuration, StbEosProcessor.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        StbEosProcessor.create_processor(configuration)
    ).start()
