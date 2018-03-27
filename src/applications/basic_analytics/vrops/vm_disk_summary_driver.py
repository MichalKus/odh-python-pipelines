from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg


class VMDiskSummaryProcessor(BasicAnalyticsProcessor):
    """
    VROPS Virtual Machine Processor for averaging Disk/Virtual Disk/Summary stats
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

        def aggregate(aggregation_field, group):
            """
            Build aggregated stream for each metric
            :param metric_name: name of mem/net metric which needs to be averaged.
            :return: list of streams
            """
            aggregation = Avg(group_fields=["res_kind", "group", "name"], aggregation_field=aggregation_field,
                              aggregation_name=self._component_name)
            agg_stream = read_stream \
                .select("@timestamp", "group", "res_kind", "name",
                        col("metrics.{}".format(aggregation_field)).alias(aggregation_field)) \
                .filter(
                (col("group") == group) & (col("res_kind") == "VirtualMachine") & (col(aggregation_field).isNotNull())) \
                .withColumn("name", regexp_replace("name", r"\.", "-")) \
                .aggregate(aggregation)

            return agg_stream

        freespace_total = aggregate("freespace_total", "guestfilesystem")
        percentage_total = aggregate("percentage_total", "guestfilesystem")
        capacity_total = aggregate("capacity_total", "guestfilesystem")
        usage_total = aggregate("usage_total", "guestfilesystem")
        disk_usage_average = aggregate("usage_average", "disk")
        virtualdisk_write_average = aggregate("write_average", "virtualdisk")
        virtualdisk_read_average = aggregate("read_average", "virtualdisk")
        summary_workload_indicator = aggregate("workload_indicator", "summary")

        return [freespace_total, percentage_total, capacity_total, usage_total, disk_usage_average,
                virtualdisk_write_average, virtualdisk_read_average, summary_workload_indicator]

    @staticmethod
    def create_schema():
        """
        Schema for input stream.
        """
        return StructType([
            StructField("metrics", StructType([
                StructField("freespace_total", DoubleType()),
                StructField("percentage_total", DoubleType()),
                StructField("capacity_total", DoubleType()),
                StructField("usage_total", DoubleType()),
                StructField("usage_average", DoubleType()),
                StructField("write_average", DoubleType()),
                StructField("read_average", DoubleType()),
                StructField("workload_indicator", DoubleType())
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
    return VMDiskSummaryProcessor(configuration, VMDiskSummaryProcessor.create_schema())

if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
