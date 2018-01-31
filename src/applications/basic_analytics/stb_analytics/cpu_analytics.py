import sys
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
from common.kafka_pipeline import KafkaPipeline
import statistics
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.utils import Utils


__author__ = "John Gregory Stockton"
__maintainer__ = "John Gregory Stockton"
__copyright__ = "Liberty Global"
__email__ = "jstockton@libertyglobal.com"
__status__ = "Pre-Prod"

logging.basicConfig(level=logging.WARN)


class BasicAnalyticsCPU(BasicAnalyticsProcessor):
    """This class expose method useful for cpu metrics"""
    """def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration.property("analytics.componentName")
        self.kafka_output = configuration.property("kafka.topics.output")"""


    def _process_pipeline(self, read_stream):

        idle_pct_avg = read_stream.withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", "5 minutes"), "json.firmwareVersion", "json.appVersion", "json.modelDescription") \
            .agg(((avg("json.VMStat_idlePct")).alias("VMStat_idlePct")))

        system_pct =  read_stream.withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", "5 minutes"), "json.firmwareVersion", "json.appVersion", "json.modelDescription") \
            .agg(((avg("json.VMStat_systemPct"))))

        iowait_pct = read_stream.withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", "5 minutes"), "json.firmwareVersion", "json.appVersion", "json.modelDescription") \
            .agg(((avg("json.VMStat_iowaitPct"))))

        hwIrqPct = read_stream.withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", "5 minutes"), "json.firmwareVersion", "json.appVersion", "json.modelDescription") \
            .agg(((avg("json.VMStat_hwIrqPct"))))

        mem_usage = read_stream.withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", "5 minutes"), "json.firmwareVersion", "json.appVersion", "json.modelDescription") \
            .agg(((avg("json.MemoryUsage_totalKb"))))

        return [system_pct, idle_pct_avg, iowait_pct, hwIrqPct, mem_usage]


    def construct_metric_system_pct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("systemPct_avg"))

    def construct_metric_idle_pct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("idlePct_avg"))

    def construct_metric_iowait_pct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("iowaitPct"))

    def construct_metric_hwIrqPct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("hwIrqPct"))

    def construct_metric_mem_usage (self, dataframe):

        return dataframe.withColumn("metric_name", lit("mem_usage"))

    def create(self, read_stream):

        json_stream = read_stream.select(read_stream["timestamp"].cast("timestamp").alias("timestamp"),
                        from_json(read_stream["value"].cast("string"),
                        self.get_message_schema()).alias("json"))

        dataframes = self._process_pipeline(json_stream)

        dataframes_output = []

        dataframes_output.append(self.construct_metric_system_pct((dataframes[0])))

        dataframes_output.append((self.construct_metric_idle_pct((dataframes[1]))))

        dataframes_output.append((self.construct_metric_iowait_pct((dataframes[2]))))

        dataframes_output.append((self.construct_metric_hwIrqPct((dataframes[3]))))

        dataframes_output.append((self.construct_metric_mem_usage(dataframes[4])))

        return [self.convert_to_kafka_structure(dataframe) for dataframe in dataframes_output]


    def convert_to_kafka_structure(self, dataframe):

        return dataframe \
            .withColumn("@timestamp", col("window.start")) \
            .drop("window") \
            .selectExpr("metric_name","to_json(struct(*)) AS value")\
            .withColumn("topic", lit((self.__configuration.property("kafka.topics.output"))))


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
    """return basicAnalytics cpu istance"""
    return BasicAnalyticsCPU(configuration, BasicAnalyticsCPU.get_message_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
