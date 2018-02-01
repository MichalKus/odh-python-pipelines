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

#logging.basicConfig(level=logging.WARN)


class StbAnalyticsCPU(BasicAnalyticsProcessor):
    """This class expose method useful for cpu metrics"""

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "modelDescription"]

    def _process_pipeline(self, read_stream):

        idle_pct = read_stream \
            .withColumn("VMStat_idlePct", read_stream["VMStat_idlePct"].cast("Int")) \
            .aggregate(Avg(group_fields=["hardwareVersion", "firmwareVersion",  "appVersion" , "modelDescription"], aggregation_field="VMStat_idlePct", aggregation_name=self._component_name))

        return [idle_pct]


    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
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
