from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg


class VMDatastoreProcessor(BasicAnalyticsProcessor):
    """
    VROPS Virtual Machine MEM NET Processor for averaging net stats
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

        def aggregate(aggregation_field):
            """
            Build aggregated stream for each metric
            :param metric_name: name of mem/net metric which needs to be averaged.
            :return: list of streams
            """
            aggregation = Avg(group_fields=["res_kind", "group", "name"], aggregation_field=aggregation_field,
                              aggregation_name=self._component_name)
            agg_stream = read_stream \
                .select("@timestamp", "group", "res_kind", "name", "metrics.*") \
                .select("@timestamp", "group", "res_kind", "name", aggregation_field) \
                .filter(
                (col("group") == "datastore") & (col("res_kind") == "VirtualMachine") & (col(aggregation_field).isNotNull())) \
                .withColumn("name", regexp_replace("name", r"\.", "-")) \
                .aggregate(aggregation)

            return agg_stream

        numberreadaveraged_average = aggregate("numberreadaveraged_average")
        numberwriteaveraged_average = aggregate("numberwriteaveraged_average")
        read_average = aggregate("read_average")
        totallatency_average = aggregate("totallatency_average")
        totalwritelatency_average = aggregate("totalwritelatency_average")
        write_average = aggregate("write_average")

        return [numberreadaveraged_average, numberwriteaveraged_average, read_average, totallatency_average,
                totalwritelatency_average, write_average]

    @staticmethod
    def create_schema():
        """
        Schema for input stream.
        """
        return StructType([
            StructField("metrics", StructType([
                StructField("numberreadaveraged_average", DoubleType()),
                StructField("numberwriteaveraged_average", DoubleType()),
                StructField("read_average", DoubleType()),
                StructField("totallatency_average", DoubleType()),
                StructField("totalreadlatency_average", DoubleType()),
                StructField("totalwritelatency_average", DoubleType()),
                StructField("write_average", DoubleType()),
            ])),
            StructField("group", StringType()),
            StructField("name", StringType()),
            StructField("timestamp", StringType()),
            StructField("res_kind", StringType())
        ])


def create_processor(configuration):
    """
    Method to create the instance of the processor
    :param configuration: dict containing configurations
    """
    return VMDatastoreProcessor(configuration, VMDatastoreProcessor.create_schema())

if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
