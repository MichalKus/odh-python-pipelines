import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import *
from common.basic_analytics.aggregations import Count
from util.utils import Utils


class AirflowWorker(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        exception_types_stream = read_stream \
            .withColumn("exception_text", regexp_extract("message", "\nException\:(.+?)\n", 1)) \
            .where("exception_text != ''") \
            .withColumn("exception_type",
                        custom_translate_regex(
                            source_field=col("exception_text"),
                            mapping={
                                ".*Failed to connect to.*": "connection_failed",
                                ".*Bad checksum for the.*": "bad_checksum",
                                ".*packages moved to failed folder.*": "movement_to_failed_folder",
                                ".*managed folder doesn't exist.*": "folder_does_not_exist"
                            },
                            default_value="unclassified")) \
            .aggregate(Count(group_fields=["exception_type"], aggregation_name=self._component_name))
        return [exception_types_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("script", StringType()),
            StructField("level", StringType()),
            StructField("message", StringType())
        ])


def create_processor(configuration):
    return AirflowWorker(configuration, AirflowWorker.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
