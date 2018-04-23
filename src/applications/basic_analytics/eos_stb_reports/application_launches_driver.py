from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from common.basic_analytics.aggregations import Count, Sum, Max, Min, Stddev, CompoundAggregation
from common.basic_analytics.aggregations import P01, P05, P10, P25, P50, P75, P90, P95, P99
from pyspark.sql.functions import col, regexp_replace
from common.spark_utils.custom_functions import convert_epoch_to_iso


class StbAppLaunches(BasicAnalyticsProcessor):
    """This class expose method useful for cpu metrics and memory"""

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _process_pipeline(self, read_stream):
        """This define the aggregation fields and re-use statistical functions from aggregation.py"""
        stream = read_stream \
            .withColumn("firmwareVersion", regexp_replace("firmwareVersion", r"\.", "-")) \
            .withColumn("hardwareVersion", regexp_replace("hardwareVersion", r"\.", "-")) \
            .withColumn("appVersion", regexp_replace("appVersion", r"\.", "-")) \
            .withColumn("asVersion", regexp_replace("asVersion", r"\.", "-")) \
            .withColumn("VMStat_idlePct", col("VMStat_idlePct").cast(IntegerType())) \
            .withColumn("VMStat_systemPct", col("VMStat_systemPct").cast(IntegerType())) \
            .withColumn("VMStat_iowaitPct", col("VMStat_iowaitPct").cast(IntegerType())) \
            .withColumn("VMStat_hwIrqPct", col("VMStat_hwIrqPct").cast(IntegerType())) \
            .withColumn("MemoryUsage_freeKb", col("MemoryUsage_freeKb").cast(IntegerType())) \
            .withColumn("MemoryUsage_cachedKb", col("MemoryUsage_cachedKb").cast(IntegerType())) \
            .withColumn("MemoryUsage_usedKb", col("MemoryUsage_usedKb").cast(IntegerType())) \
            .withColumn("VMStat_nicePct", col("VMStat_nicePct").cast(IntegerType())) \
            .withColumn("VMStat_userPct", col("VMStat_userPct").cast(IntegerType())) \
            .withColumn("VMStat_swIrqPct", col("VMStat_swIrqPct").cast(IntegerType())) \
            .withColumn("VMStat_loadAverage", col("VMStat_loadAverage").cast(IntegerType()))

        aggregation_fields = ["VMStat_idlePct", "VMStat_systemPct", "VMStat_iowaitPct", "VMStat_hwIrqPct",
                              "MemoryUsage_usedKb", "MemoryUsage_freeKb", "MemoryUsage_cachedKb",
                              "VMStat_nicePct","VMStat_userPct", "VMStat_swIrqPct", "VMStat_loadAverage"]

        aggregation_fields_with_sum = ["MemoryUsage_usedKb", "MemoryUsage_freeKb", "MemoryUsage_cachedKb"]

        aggregations = []
        for field in aggregation_fields:
            kwargs = {'aggregation_field': field}

            aggregations.extend([Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                                 P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                                 P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)])

            if kwargs["aggregation_field"] in aggregation_fields_with_sum:
                aggregations.append(Sum(**kwargs))

        return [stream.aggregate(CompoundAggregation(aggregations=aggregations, group_fields=self.__dimensions,
                                                     aggregation_name=self._component_name))]

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    @staticmethod
    def create_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("originId", StringType()),
            StructField("MemoryUsage_usedKb", StringType()),
            StructField("MemoryUsage_freeKb", StringType()),
            StructField("MemoryUsage_cachedKb", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("modelDescription", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("VMStat_idlePct", StringType()),
            StructField("VMStat_iowaitPct", StringType()),
            StructField("VMStat_systemPct", StringType()),
            StructField("VMStat_swIrqPct", StringType()),
            StructField("VMStat_hwIrqPct", StringType()),
            StructField("VMStat_nicePct", StringType()),
            StructField("VMStat_userPct", StringType()),
            StructField("VMStat_loadAverage", StringType())
        ])


def create_processor(configuration):
    """Create Processor"""
    return StbAppLaunches(configuration, StbAppLaunches.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
