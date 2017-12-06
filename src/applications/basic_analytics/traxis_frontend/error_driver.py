import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import *
from common.basic_analytics.aggregations import Count
from util.utils import Utils


class TraxisFrontendError(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        error_stream = read_stream.where("level = 'ERROR'") \
            .withColumn("counter",
                        custom_translate_like(
                            source_field=col("message"),
                            mappings_pair=[
                                (["Eventis.Traxis.Cassandra.CassandraException"], "traxis_cassandra_error"),
                                (["NetworkTimeCheckError"], "ntp_error")
                            ],
                            default_value="unclassifed_errors"))
        warn_and_fatal_stream = read_stream.where("level in ('WARN', 'FATAL')") \
            .withColumn("counter", lit("unclassifed_errors"))

        result_stream = error_stream.union(warn_and_fatal_stream) \
            .aggregate(Count(group_fields=["hostname", "counter"],
                             aggregation_name=self._component_name))

        return [result_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("thread_name", StringType()),
            StructField("component", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    return TraxisFrontendError(configuration, TraxisFrontendError.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
