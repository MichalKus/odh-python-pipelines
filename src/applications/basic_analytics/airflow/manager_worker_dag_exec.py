from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, BooleanType, ArrayType
import re

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class AirflowWorkerDagExec(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to DAGs in the Airflow Worker component.
    """

    def _manager_filter(self, stream):
        """
        filter airflow manager logs
        :param stream: input streams
        """

        def regex_filter(x):
            """
            Check if message match regex expression
            :param x: msg string
            :return: Boolean
            """
            regexs = [r"Created.*DagRun\s(?P<tenant>\w{2,3})_(?P<dag>.*)\s@",
                      r"Marking.*DagRun\s(?P<dag>.*)\s@.*(?P<tenant>\w{2,3})-crid.*\s(?P<type>\w+)$",
                      r"Marking.*DagRun\s(?P<tenant>\w{2,3})_(?P<dag>.*)\s@.*\s(?P<type>\w+)$"]

            if x and x.strip():
                for r in regexs:
                    if re.search(r, x):
                        return True

            return False

        regex_filter_udf = udf(regex_filter, BooleanType())

        filtered_airflow_manager = stream \
            .where(col("topic").isNotNull()) \
            .where(regex_filter_udf(col("message")))

        return filtered_airflow_manager


    def _manager_process(self, stream):
        """
        process airflow manager logs
        :param stream: input streams
        """

        def regex_extract(x):
            """
            Check if message match regex expression
            :param x: msg string
            :return: group matched list
            """
            regexs = [r"Created.*DagRun\s(?P<tenant>\w{2,3})_(?P<dag>.*)\s@",
                      r"Marking.*DagRun\s(?P<dag>.*)\s@.*(?P<tenant>\w{2,3})-crid.*\s(?P<type>\w+)$",
                      r"Marking.*DagRun\s(?P<tenant>\w{2,3})_(?P<dag>.*)\s@.*\s(?P<type>\w+)$"]

            for r in regexs:
                match = re.search(r, x)
                if match:
                    groups = match.groups()
                    if len(groups[0]) > 3:
                        return [groups[1], groups[0], groups[2]]
                    elif len(groups) == 2:
                        return list(groups) + ["initiated"]
                    else:
                        return list(groups)

        tenant = self._component_name.split(".")[2]
        regex_extract_udf = udf(regex_extract, ArrayType(StringType()))

        filtered = self._manager_filter(stream)

        airflow_manager = filtered \
            .withColumn("message",
                        regex_extract_udf(col("message"))) \
            .withColumn('tenant', col("message").getItem(0)) \
            .withColumn('dag', col("message").getItem(1)) \
            .withColumn('type', col("message").getItem(2)) \
            .withColumn('count', lit(1)) \
            .withColumn("tenant",
                        when((col("tenant") == "uk") | (col("tenant") == "bbc"), "gb").otherwise(col("tenant"))) \
            .where(col("tenant") == tenant) \
            .select("@timestamp", "dag", "type", "count")

        return airflow_manager

    def _pre_process(self, stream):
        """
        Process stream before output
        :param stream: input stream
        """

        def remove_tenant_prefix(x):
            """
            Remove tenant prefix from dag name
            :param x: msg string
            :return: string
            """
            regex = r"^\w{2,3}_(?P<dag>.*)"
            match = re.search(regex, x)
            if match:
                return match.groups()[0]
            else:
                return x

        remove_prefix_udf = udf(remove_tenant_prefix, StringType())

        airflow_worker_dag = stream \
            .where(col("dag").isNotNull()) \
            .withColumn("dag", remove_prefix_udf(col("dag"))) \
            .withColumn('count', lit(1)) \
            .select("@timestamp", "dag", "hostname", "task", "count")

        return [self._manager_process(stream), airflow_worker_dag]

    def _agg_airflow_manager(self, streams):
        """
        Aggregate uservice - he component call counts
        :param stream:
        :return:
        """
        agg_manager = Count(group_fields=["dag", "type"], aggregation_field="count",
                            aggregation_name=self._component_name + ".manager")

        agg_worker = Count(group_fields=["dag", "hostname", "task"], aggregation_field="count",
                            aggregation_name=self._component_name + ".worker")

        manager = streams[0].aggregate(agg_manager)
        worker = streams[1].aggregate(agg_worker)

        return [manager, worker]

    def _process_pipeline(self, read_stream):
        """
        Process stream via filtering and aggregating
        :param read_stream: input stream
        """
        pre_processed_streams = self._pre_process(read_stream)

        return self._agg_airflow_manager(pre_processed_streams)

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("message", StringType()),
            StructField("topic", StringType()),
            StructField("hostname", StringType()),
            StructField("task", StringType()),
            StructField("dag", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return AirflowWorkerDagExec(configuration, AirflowWorkerDagExec.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
