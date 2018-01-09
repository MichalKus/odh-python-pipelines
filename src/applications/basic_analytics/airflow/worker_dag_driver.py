"""
The module for the driver to calculate metrics related to DAGs in the Airflow Worker component.
See:
  ODH-1439: Success/failure extension of Airflow-Worker basics analytics job
  ODH-1442: Airflow. Running DAGs per hosts
"""

from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, DistinctCount
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class AirflowWorkerDag(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to DAGs in the Airflow Worker component.
    """

    def _process_pipeline(self, read_stream):
        dag_count = read_stream \
            .select(col("hostname"), col("@timestamp"), col("dag")) \
            .aggregate(DistinctCount(group_fields=["hostname"], aggregation_field="dag",
                                     aggregation_name=self._component_name))

        success_and_failures_counts = read_stream \
            .select(col("@timestamp"), col("task"), col("dag"), col("message")) \
            .where(col("message").like("Task exited with return code%")) \
            .withColumn("status",
                        when(col("message").like("Task exited with return code 0%"), lit("success"))
                        .otherwise(lit("failure"))) \
            .aggregate(Count(group_fields=["task", "dag", "status"], aggregation_name=self._component_name))

        return [dag_count, success_and_failures_counts]

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
    """Method to create the instance of the processor"""

    return AirflowWorkerDag(configuration, AirflowWorkerDag.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
