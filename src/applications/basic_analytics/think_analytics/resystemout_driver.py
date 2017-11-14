import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from util.utils import Utils


class ThinkAnalyticsReSystemOut(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        duration_stream = read_stream \
            .where("level == 'INFO'") \
            .select(
            read_stream["@timestamp"],
            regexp_extract(read_stream["message"], '^.*?(\d+)ms.$', 1).cast("Int").alias("duration")) \
            .aggregate(Avg(aggregation_field="duration", aggregation_name=self._component_name))

        return [duration_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("script", StringType()),
            StructField("message", StringType())
        ])


def create_processor(configuration):
    return ThinkAnalyticsReSystemOut(configuration, ThinkAnalyticsReSystemOut.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
