"""
Modules contains general logic for HE components like ODH
"""

from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class EosStbOdhProcessor(BasicAnalyticsProcessor):
    """
    STB EOS ODH driver contains logic for calculation several metrics:
    - count
    - unique_count
    """

    def __init__(self, configuration, schema):
        super(EosStbOdhProcessor, self).__init__(configuration, schema, "ts")

    def _prepare_stream(self, read_stream):
        return read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.header.*") \
            .select(col("viewerId").alias("stb_id"),
                    from_unixtime(col("ts") / 1000).cast(TimestampType()).alias("ts")) \
            .withWatermark(self._timefield_name, self._get_interval_duration("watermark"))

    def _process_pipeline(self, read_stream):
        stb_ids_count = read_stream.aggregate(
            Count(aggregation_field="stb_id",
                  aggregation_name=self._component_name))

        stb_ids_distinct_count = read_stream.aggregate(
            DistinctCount(aggregation_field="stb_id",
                          aggregation_name=self._component_name))
        return [stb_ids_count, stb_ids_distinct_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("ts", LongType()),
                StructField("viewerID", StringType()),
            ]))
        ])

    @staticmethod
    def create_processor(configuration):
        return EosStbOdhProcessor(configuration, EosStbOdhProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(EosStbOdhProcessor.create_processor)
