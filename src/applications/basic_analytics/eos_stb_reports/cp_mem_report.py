from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from common.basic_analytics.aggregations import Count, Sum, Max, Min, Stddev, CompoundAggregation
from common.basic_analytics.aggregations import P01, P05, P10, P25, P50, P75, P90, P95, P99
from pyspark.sql.functions import col, regexp_replace
from common.spark_utils.custom_functions import convert_epoch_to_iso

__author__ = "John Gregory Stockton"
__maintainer__ = "John Gregory Stockton"
__copyright__ = "Liberty Global"
__email__ = "jstockton@libertyglobal.com"
__status__ = "Pre-Prod"


class StbAnalyticsCPU(BasicAnalyticsProcessor):
    """This class expose method useful for cpu metrics and memory"""

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _process_pipeline(self, read_stream):

        stream = read_stream \
            .withColumn("firmwareVersion", regexp_replace("firmwareVersion", "\.", "-")) \
            .withColumn("hardwareVersion", regexp_replace("hardwareVersion", "\.", "-")) \
            .withColumn("appVersion", regexp_replace("appVersion", "\.", "-")) \
            .withColumn("asVersion", regexp_replace("asVersion", "\.", "-")) \
            .withColumn("VMStat_idlePct", col("VMStat_idlePct").cast(IntegerType())) \
            .withColumn("VMStat_systemPct", col("VMStat_systemPct").cast(IntegerType())) \
            .withColumn("VMStat_iowaitPct", col("VMStat_iowaitPct").cast(IntegerType())) \
            .withColumn("VMStat_hwIrqPct", col("VMStat_hwIrqPct").cast(IntegerType())) \
            .withColumn("MemoryUsage_totalKb", col("MemoryUsage_totalKb").cast(IntegerType())) \
            .withColumn("MemoryUsage_totalKb", col("MemoryUsage_totalKb").cast(IntegerType()))

        aggregation_fields = ["VMStat_idlePct", "VMStat_systemPct", "VMStat_iowaitPct", "VMStat_hwIrqPct",
                              "MemoryUsage_totalKb", "MemoryUsage_totalKb"]
        result = []

        for field in aggregation_fields:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field}

            aggregations = [Sum(**kwargs), Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                            P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                            P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

            result.append(stream.aggregate(CompoundAggregation(aggregations=aggregations, **kwargs)))

        return result

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    @staticmethod
    def create_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("originId", StringType()),
            StructField("MemoryUsage_freeKb", StringType()),
            StructField("MemoryUsage_totalKb", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("modelDescription", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("VMStat_idlePct", StringType()),
            StructField("VMStat_iowaitPct", StringType()),
            StructField("VMStat_systemPct", StringType()),
            StructField("VMStat_swIrqPct", StringType()),
            StructField("VMStat_hwIrqPct", StringType())
        ])


def create_processor(configuration):
    """Create Processor"""
    return StbAnalyticsCPU(configuration, StbAnalyticsCPU.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
