"""
Module for counting all general analytics metrics for EOS STB VMStats Report
"""
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, ArrayType, \
    LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg, DistinctCount
from pyspark.sql.functions import col, explode


class VmStatReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for VMStats Reports
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "VMStat.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        self._time_in_percents = ".time_in_percents"

        self._common_vm_stat_pipeline = read_stream \
            .select("@timestamp",
                    "VMStat.*",
                    col("header.viewerID").alias("viewer_id"),
                    col("header.softwareVersions").alias("software_versions"))

        return [self.average_uptime_across_stb(),
                self.average_usage_hardware_interrupt(),
                self.average_usage_low_priority_mode(),
                self.average_user_active_mode(),
                self.restarted_stbs_total_count(),
                self.restarted_stbs_count_per_firmware(),
                self.average_usage_cpu_in_wait(),
                self.average_usage_system_mode(),
                self.average_software_interrupt()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("VMStat", StructType([
                StructField("ts", LongType()),
                StructField("uptime", IntegerType()),
                StructField("hwIrqPct", DoubleType()),
                StructField("iowaitPct", DoubleType()),
                StructField("swIrqPct", DoubleType()),
                StructField("systemPct", DoubleType()),
                StructField("userPct", DoubleType()),
                StructField("nicePct", DoubleType())
            ])),
            StructField("header", StructType([
                StructField("viewerID", StringType()),
                StructField("softwareVersions", ArrayType(
                    StructType([
                        StructField("version", StringType())
                    ])
                ))
            ]))
        ])

    def average_uptime_across_stb(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("uptime").alias("uptime_sec")) \
            .aggregate(Avg(aggregation_field="uptime_sec",
                           aggregation_name=self._component_name))

    def average_usage_hardware_interrupt(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("hwIrqPct")) \
            .aggregate(Avg(aggregation_field="hwIrqPct",
                           aggregation_name=self._component_name + self._time_in_percents))

    def average_usage_cpu_in_wait(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("iowaitPct")) \
            .aggregate(Avg(aggregation_field="iowaitPct",
                           aggregation_name=self._component_name + self._time_in_percents))

    def average_usage_low_priority_mode(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("nicePct")) \
            .aggregate(Avg(aggregation_field="nicePct",
                           aggregation_name=self._component_name + self._time_in_percents))

    def average_usage_system_mode(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("systemPct")) \
            .aggregate(Avg(aggregation_field="systemPct",
                           aggregation_name=self._component_name + self._time_in_percents))

    def average_user_active_mode(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("userPct")) \
            .aggregate(Avg(aggregation_field="userPct",
                           aggregation_name=self._component_name + self._time_in_percents))

    def average_software_interrupt(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("swIrqPct")) \
            .aggregate(Avg(aggregation_field="swIrqPct",
                           aggregation_name=self._component_name + self._time_in_percents))

    def restarted_stbs_total_count(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", "uptime", "viewer_id") \
            .where((col("uptime") >= 0) & (col("uptime") <= 3600)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".restarted_stbs"))

    def restarted_stbs_count_per_firmware(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", "uptime", "viewer_id", explode("software_versions.version")
                    .alias("software_version")) \
            .where((col("uptime") >= 0) & (col("uptime") <= 3600)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["software_version"],
                                     aggregation_name=self._component_name + ".restarted_stbs_per_frimware"))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return VmStatReportEventProcessor(configuration, VmStatReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
