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

        time_in_percents = ".time_in_percents"

        common_vm_stat_pipeline = read_stream \
            .select("@timestamp",
                    "VMStat.*",
                    col("header.viewerID").alias("viewer_id"),
                    col("header.softwareVersions").alias("software_versions"))

        return [self.__average_uptime_across_stb(common_vm_stat_pipeline),
                self.__average_usage_hardware_interrupt(common_vm_stat_pipeline, time_in_percents),
                self.__average_usage_low_priority_mode(common_vm_stat_pipeline, time_in_percents),
                self.__average_user_active_mode(common_vm_stat_pipeline, time_in_percents),
                self.__restarted_stbs_total_count(common_vm_stat_pipeline),
                self.__restarted_stbs_count_per_firmware(common_vm_stat_pipeline),
                self.__average_usage_cpu_in_wait(common_vm_stat_pipeline, time_in_percents),
                self.__average_usage_system_mode(common_vm_stat_pipeline, time_in_percents),
                self.__average_software_interrupt(common_vm_stat_pipeline, time_in_percents)]

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

    def __average_uptime_across_stb(self, common_vm_stat_pipeline):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("uptime").alias("uptime_sec")) \
            .aggregate(Avg(aggregation_field="uptime_sec",
                           aggregation_name=self._component_name))

    def __average_usage_hardware_interrupt(self, common_vm_stat_pipeline, time_in_percents):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("hwIrqPct")) \
            .aggregate(Avg(aggregation_field="hwIrqPct",
                           aggregation_name=self._component_name + time_in_percents))

    def __average_usage_cpu_in_wait(self, common_vm_stat_pipeline, time_in_percents):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("iowaitPct")) \
            .aggregate(Avg(aggregation_field="iowaitPct",
                           aggregation_name=self._component_name + time_in_percents))

    def __average_usage_low_priority_mode(self, common_vm_stat_pipeline, time_in_percents):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("nicePct")) \
            .aggregate(Avg(aggregation_field="nicePct",
                           aggregation_name=self._component_name + time_in_percents))

    def __average_usage_system_mode(self, common_vm_stat_pipeline, time_in_percents):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("systemPct")) \
            .aggregate(Avg(aggregation_field="systemPct",
                           aggregation_name=self._component_name + time_in_percents))

    def __average_user_active_mode(self, common_vm_stat_pipeline, time_in_percents):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("userPct")) \
            .aggregate(Avg(aggregation_field="userPct",
                           aggregation_name=self._component_name + time_in_percents))

    def __average_software_interrupt(self, common_vm_stat_pipeline, time_in_percents):
        return common_vm_stat_pipeline \
            .select("@timestamp", col("swIrqPct")) \
            .aggregate(Avg(aggregation_field="swIrqPct",
                           aggregation_name=self._component_name + time_in_percents))

    def __restarted_stbs_total_count(self, common_vm_stat_pipeline):
        return common_vm_stat_pipeline \
            .select("@timestamp", "uptime", "viewer_id") \
            .where((col("uptime") >= 0) & (col("uptime") <= 100)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".restarted_stbs",
                                     aggregation_window=self._get_interval_duration("uniqCountWindow")))

    def __restarted_stbs_count_per_firmware(self, common_vm_stat_pipeline):
        return common_vm_stat_pipeline \
            .select("@timestamp", "uptime", "viewer_id", explode("software_versions.version")
                    .alias("software_version")) \
            .where((col("uptime") >= 0) & (col("uptime") <= 100)) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["software_version"],
                                     aggregation_name=self._component_name + ".restarted_stbs_per_frimware",
                                     aggregation_window=self._get_interval_duration("uniqCountWindow")))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return VmStatReportEventProcessor(configuration, VmStatReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
