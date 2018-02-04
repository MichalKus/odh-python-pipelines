from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType
from pyspark.sql.functions import from_unixtime
from common.basic_analytics.aggregations import Count, Sum, Max, Min, Avg
from pyspark.sql.functions import col

__author__ = "John Gregory Stockton"
__maintainer__ = "John Gregory Stockton"
__copyright__ = "Liberty Global"
__email__ = "jstockton@libertyglobal.com"
__status__ = "Pre-Prod"

#logging.basicConfig(level=logging.WARN)


class StbAnalyticsCPU(BasicAnalyticsProcessor):
    """This class expose method useful for cpu metrics"""

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "modelDescription"]

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("timestamp") / 1000).cast(TimestampType()))

    def _process_pipeline(self, read_stream):

        read_stream = self._prepare_timefield(read_stream)

        idle_pct = read_stream \
            .withColumn("VMStat_idlePct", read_stream["VMStat_idlePct"].cast("Int")) \
            .aggregate(Avg(group_fields=["hardwareVersion", "firmwareVersion",  "appVersion", "modelDescription"],
                           aggregation_field="VMStat_idlePct",
                           aggregation_name=self._component_name))

        system_pct = read_stream \
            .withColumn("VMStat_systemPct", read_stream["VMStat_systemPct"].cast("Int")) \
            .aggregate(Avg(group_fields=["hardwareVersion", "firmwareVersion",  "appVersion", "modelDescription"],
                           aggregation_field="VMStat_systemPct",
                           aggregation_name=self._component_name))

        iowait_pct = read_stream \
            .withColumn("VMStat_iowaitPct", read_stream["VMStat_iowaitPct"].cast("Int")) \
            .aggregate(Avg(group_fields=["hardwareVersion", "firmwareVersion", "appVersion", "modelDescription"],
                             aggregation_field="VMStat_iowaitPct",
                             aggregation_name=self._component_name))

        hwIrq_pct = read_stream \
            .withColumn("VMStat_hwIrqPct", read_stream["VMStat_hwIrqPct"].cast("Int")) \
            .aggregate(Avg(group_fields=["hardwareVersion", "firmwareVersion", "appVersion", "modelDescription"],
                           aggregation_field="VMStat_hwIrqPct",
                           aggregation_name=self._component_name))

        mem_usage = read_stream \
            .withColumn("MemoryUsage_totalKb", read_stream["MemoryUsage_totalKb"].cast("Int")) \
            .aggregate(Avg(group_fields=["hardwareVersion", "firmwareVersion", "appVersion", "modelDescription"],
                           aggregation_field="MemoryUsage_totalKb",
                           aggregation_name=self._component_name))

        read_stream.writeStream.format("console").outputMode("update").start()
        #read_stram.writeStream()

        return [idle_pct, system_pct, iowait_pct, hwIrq_pct, mem_usage]


    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
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
