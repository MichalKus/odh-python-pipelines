from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg


class VMCpuProcessor(BasicAnalyticsProcessor):
    """
    VROPS Virtual Machine CPU Processor for averaging cpu stats
    """

    def _prepare_timefield(self, data_stream):
        """
        Convert to appropriate timestamp type
        :param data_stream: input stream
        """
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    def _process_pipeline(self, read_stream):
        """
        Process stream via filtering and aggregating
        :param read_stream: input stream
        """

        def for_each_metric(metric_name):
            """
            Build aggregated stream for each metric
            :param metric_name: name of cpu metric which needs to be averaged.
            :return: list of streams
            """
            aggregation = Avg(group_fields=["res_kind", "group", "name"], aggregation_field=metric_name,
                              aggregation_name=self._component_name)
            agg_stream = read_stream \
                .select("@timestamp", "group", "res_kind", "name", "metrics.*") \
                .select("@timestamp", "group", "res_kind", "name", metric_name) \
                .filter(
                (col("group") == "cpu") & (col("res_kind") == "VirtualMachine")  & (col(metric_name).isNotNull())) \
                .aggregate(aggregation)

            return agg_stream

        return map(for_each_metric, ["demandpct", "idlepct", "readypct", "swapwaitpct", "usage_average"])

    @staticmethod
    def create_schema():
        """
        Schema for input stream.
        """
        return StructType([
            StructField("timestamp", StringType()),
            StructField("group", StringType()),
            StructField("res_kind", StringType()),
            StructField("name", StringType()),
            StructField("metrics", StructType([
                StructField("demandpct", DoubleType()),
                StructField("idlepct", DoubleType()),
                StructField("readypct", DoubleType()),
                StructField("swapwaitpct", DoubleType()),
                StructField("usage_average", DoubleType())
            ]))
        ])


def create_processor(configuration):
    """
    Method to create the instance of the processor
    :param configuration: dict containing configurations
    """
    return VMCpuProcessor(configuration, VMCpuProcessor.create_schema())

if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
