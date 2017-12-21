import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import *
from util.utils import Utils


class AirflowWorkerDag(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):

        dag_count = read_stream \
            .select(col("hostname"), col("@timestamp"), col("dag")) \
            .aggregate(DistinctCount(group_fields=["hostname"], aggregation_field="dag",
                                     aggregation_name=self._component_name))

        return [dag_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("dag", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    return AirflowWorkerDag(configuration, AirflowWorkerDag.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
