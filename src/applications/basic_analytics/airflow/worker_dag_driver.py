import sys

from pyspark.sql.types import *
from pyspark.sql.functions import *

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.aggregations import Count

from util.utils import Utils


class AirflowWorker(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        success_and_failures_counts = read_stream \
            .select(col("@timestamp"), col("task"), col("dag"), col("message")) \
            .where(col("message").like("Task exited with return code%")) \
            .withColumn("success",
                        when(col("message").like("Task exited with return code 0%"), lit("true"))
                        .when(col("message").like("Task exited with return code 1%"), lit("false"))) \
            .aggregate(Count(group_fields=["task", "dag", "success"], aggregation_name=self._component_name))
        return [success_and_failures_counts]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("message", StringType()),
            StructField("hostname", StringType()),
            StructField("task", StringType()),
            StructField("dag", StringType())
        ])


def create_processor(configuration):
    return AirflowWorker(configuration, AirflowWorker.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
