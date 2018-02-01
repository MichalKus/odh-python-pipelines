from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType
from common.basic_analytics.aggregations import Count, Sum, Max, Min, Avg
from pyspark.sql.functions import col

__author__ = "John Gregory Stockton"
__maintainer__ = "John Gregory Stockton"
__copyright__ = "Liberty Global"
__email__ = "jstockton@libertyglobal.com"
__status__ = "Pre-Prod"

logging.basicConfig(level=logging.WARN)


class StbAnalyticsCPU(BasicAnalyticsProcessor):
    """This class expose method useful for cpu metrics"""

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _process_pipeline(self, json_stream):
        stream = json_stream \
            .withColumn("VMStat_idlePct", col("VMStat_idlePct").cast(IntegerType())) \
            .withColumn("VMStat_iowaitPct", col("VMStat_iowaitPct").cast(IntegerType())) \
            .withColumn("VMStat_systemPct", col("VMStat_systemPct").cast(IntegerType())) \
            .withColumn("VMStat_swIrqPct", col("VMStat_swIrqPct").cast(IntegerType())) \
            .withColumn("VMStat_hwIrqPct", col("VMStat_hwIrqPct").cast(IntegerType())) \
            .withColumn("MemoryUsage_totalKb", col("MemoryUsage_totalKb").cast(IntegerType())) \
            .withColumn("MemoryUsage_totalKb", col("MemoryUsage_totalKb").cast(IntegerType()))

        aggregation_fields = ["VMStat_idlePct", "VMStat_iowaitPct", "VMStat_systemPct", "VMStat_swIrqPct",
                              "VMStat_hwIrqPct", "MemoryUsage_totalKb", "MemoryUsage_totalKb"]

        for field in aggregation_fields:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field}
            aggregations += [
                # stream.aggregate(Sum(**kwargs)),
                # stream.aggregate(Count(**kwargs)),
                # stream.aggregate(Max(**kwargs)),
                # stream.aggregate(Min(**kwargs)),
                stream.aggregate(Avg(**kwargs))
            ]

        return aggregations

    @staticmethod
    def get_message_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("originId", StringType()),
            StructField("MemoryUsage_freeKb", StringType()),
            StructField("MemoryUsage_totalKb", StringType()),
            StructField("hardwareVersion", StringType()),
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
    return StbAnalyticsCPU(configuration, StbAnalyticsCPU.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
