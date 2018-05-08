"""
The module for the driver to calculate metrics related to Airflow Manager scheduler component.
"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class AirflowManagerScheduler(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Airflow Manager scheduler component.
    """

    def _process_pipeline(self, read_stream):

        return [self._processed_dags_count(read_stream),
                self._dag_total_initiated_executions(read_stream)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("status", StringType()),
            StructField("action", StringType()),
            StructField("dag", StringType())
        ])

    def _processed_dags_count(self, read_stream):
        return read_stream \
            .filter("action = 'RUN'") \
            .aggregate(Count(group_fields=["status"],
                             aggregation_name=self._component_name))

    def _dag_total_initiated_executions(self, read_stream):
        return read_stream \
            .filter("action = 'CREATE'") \
            .aggregate(Count(aggregation_name=self._component_name + ".initiated_executions"))


def create_processor(configuration):
    """Method to create the instance of the Airflow Manager Scheduler processor"""

    return AirflowManagerScheduler(configuration, AirflowManagerScheduler.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
