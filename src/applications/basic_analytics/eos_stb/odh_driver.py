"""
Modules contains general logic for HE components like ODH
"""

from pyspark.sql.functions import col, from_unixtime
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

    def __init__(self, configuration):
        BasicAnalyticsProcessor.__init__(self, configuration, EosStbOdhProcessor.create_schema())

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("header.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):
        stb_ids = read_stream.withColumn("stb_id", col("header.viewerID"))

        requests_count = stb_ids.aggregate(Count(aggregation_name=self._component_name + ".request"))
        stb_ids_distinct_count = stb_ids.aggregate(DistinctCount(aggregation_field="stb_id",
                                                                 aggregation_name=self._component_name))
        return [requests_count , stb_ids_distinct_count]

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
        return EosStbOdhProcessor(configuration)


if __name__ == "__main__":
    start_basic_analytics_pipeline(EosStbOdhProcessor.create_processor)
