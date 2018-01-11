"""
Modules contains general logic for HE components like ACS, uService and etc (exclude ODH)
"""

from pyspark.sql.functions import col, from_unixtime
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

    def __init__(self, configuration):
        BasicAnalyticsProcessor.__init__(self, configuration, GeneralEosStbProcessor.create_schema())

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("time") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):
        requests_count = read_stream.aggregate(Count(aggregation_name=self._component_name + ".request"))

        stb_ids_distinct_count = read_stream \
            .withColumn("stb_id", col("xdev")) \
            .aggregate(DistinctCount(aggregation_field="stb_id",
                                     aggregation_name=self._component_name))
        return [requests_count, stb_ids_distinct_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("time", LongType()),
            StructField("xdev", StringType()),
        ])

    @staticmethod
    def create_processor(configuration):
        return GeneralEosStbProcessor(configuration)


if __name__ == "__main__":
    start_basic_analytics_pipeline(GeneralEosStbProcessor.create_processor)
