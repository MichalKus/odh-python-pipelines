"""
Modules contains general logic for HE components like ACS, uService and etc (exclude ODH)
"""

from pyspark.sql.functions import col, from_unixtime, from_json
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class GeneralEosStbProcessor(BasicAnalyticsProcessor):
    """
    STB EOS General driver contains logic for calculation several metrics:
    - count
    - unique_count
    """

    def __init__(self, configuration, schema):
        super(GeneralEosStbProcessor, self).__init__(configuration, schema, "time")

    def _prepare_stream(self, read_stream):
        return read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*") \
            .select(col("xdev").alias("stb_id"),
                    from_unixtime(col("time") / 1000).cast(TimestampType()).alias("time")) \
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
            StructField("time", LongType()),
            StructField("xdev", StringType()),
        ])

    @staticmethod
    def create_processor(configuration):
        return GeneralEosStbProcessor(configuration, GeneralEosStbProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(GeneralEosStbProcessor.create_processor)
