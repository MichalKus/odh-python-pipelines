"""
Module for counting all general analytics metrics for EOS STB VMStats Report
"""
from pyspark.sql.types import StructField, StructType, TimestampType, IntegerType, DoubleType, StringType, ArrayType, \
    LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import Avg, DistinctCount
from pyspark.sql.functions import col, explode, from_unixtime


class VmStatReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to process pipelines for VMStats Reports
    """

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("VMStat.ts") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):

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
                self.restarted_stbs_count_per_firmware()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("VMStat", StructType([
                StructField("ts", LongType()),
                StructField("uptime", IntegerType()),
                StructField("hwIrqPct", DoubleType()),
                StructField("iowaitPct", DoubleType()),
                StructField("systemPct", DoubleType()),
                StructField("userPct", DoubleType()),
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
            .select("@timestamp", "uptime") \
            .aggregate(Avg(aggregation_field="uptime",
                           aggregation_name=self._component_name + ".uptime_across_stb"))

    def average_usage_hardware_interrupt(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("hwIrqPct").alias("hardware_interrupt")) \
            .aggregate(Avg(aggregation_field="hardware_interrupt",
                           aggregation_name=self._component_name + ".average_usage"))

    # Ask Alexey Pimenov about this metric
    # def average_usage_system_mode(self):
    #     return self._common_vm_stat_pipeline \
    #         .select("@timestamp", col("iowaitPct")) \
    #         .aggregate(Avg(aggregation_field="hardware_interrupt",
    #                        aggregation_name=self._component_name))

    def average_usage_low_priority_mode(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("iowaitPct").alias("low_priority_mode")) \
            .aggregate(Avg(aggregation_field="low_priority_mode",
                           aggregation_name=self._component_name + ".average_usage"))

    def average_usage_system_mode(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("systemPct").alias("system_mode")) \
            .aggregate(Avg(aggregation_field="system_mode",
                           aggregation_name=self._component_name + ".average_usage"))

    def average_user_active_mode(self):
        return self._common_vm_stat_pipeline \
            .select("@timestamp", col("userPct").alias("user_active_mode")) \
            .aggregate(Avg(aggregation_field="user_active_mode",
                           aggregation_name=self._component_name + ".average_usage"))

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
