import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from util.utils import Utils


class ThinkAnalyticsReIngest(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        duration_stream = read_stream \
            .where("started_script == '/apps/ThinkAnalytics/ContentIngest/bin/ingest.sh'") \
            .aggregate(Avg(aggregation_field="duration", aggregation_name=self._component_name + ".ingest"))

        return [duration_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("started_script", StringType()),
            StructField("message", StringType()),
            StructField("finished_script", StringType()),
            StructField("finished_time", TimestampType()),
            StructField("duration", StringType())
        ])


def create_processor(configuration):
    return ThinkAnalyticsReIngest(configuration, ThinkAnalyticsReIngest.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
