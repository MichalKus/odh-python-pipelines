import sys
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
from common.kafka_pipeline import KafkaPipeline
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
        self.__schema = schema
        self._component_name = configuration.property("analytics.componentName")"""

    def _process_pipeline(self, read_stream):

        """idle_pct_avg = read_stream.withWatermark("timestamp", "0 minutes") \
            .groupBy(window("timestamp", "15 minutes"), "firmwareVersion", "appVersion", "modelDescription") \
            .agg(avg("VMStat_idlePct").cast('string').alias("value"))

        system_pct = read_stream.withWatermark("timestamp", "0 minutes") \
            .groupBy(window("timestamp", "15 minutes"), "firmwareVersion", "appVersion", "modelDescription") \
            .agg(avg("VMStat_systemPct").cast('string').alias("value"))

        load_average = read_stream.withWatermark("timestamp", "0 minutes") \
            .groupBy(window("timestamp", "15 minutes"), "firmwareVersion", "appVersion", "modelDescription") \
            .agg(avg("VMStat_loadAverage").cast('string').alias("value"))"""

        #return [idle_pct_avg, system_pct, load_average]

        return [read_stream]

    def construct_metric_system_pct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("test_oboecx_pr_eosdtv_nl_prd_stb_systemPct_avg"))

    def construct_metric_idle_pct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("test_oboecx_pr_eosdtv_nl_prd_stb_idlePct_avg"))

    def construct_metric_iowait_pct(self, dataframe):

        return dataframe.withColumn("metric_name", lit("test_oboecx_pr_eosdtv_nl_prd_stb_iowaitPct"))

    #Overriding
    def create(self, read_stream):

        """json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*")"""

        json_stream = read_stream.select(read_stream["timestamp"].cast("timestamp").alias("timestamp"),
                                         from_json(read_stream["value"].cast("string"),
                                                   self.get_message_schema()).alias("json"))
        """json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"),self.get_message_schema()).alias("json")).select("json.*")"""

        dataframes = self._process_pipeline(json_stream)

        return dataframes

        """dataframesOutput = []

        dataframesOutput.append(self.construct_metric_systempct((dataframes[0])))

        dataframesOutput.append((self.construct_metric_idle_pct((dataframes[1]))))

        dataframesOutput.append((self.construct_metric_uptime((dataframes[2]))))

        dataframesOutput.append((self.construct_metric_iowait_pct((dataframes[3]))))"""


        """return [self.convert_to_kafka_structure(dataframe) for dataframe in dataframesOutput]"""



    #Overriding
    def convert_to_kafka_structure(self, dataframe):

        return dataframe \
            .withColumn("@timestamp", col("window.start")) \
            .drop("window") \
            .selectExpr("metric_name","to_json(struct(*)) AS value")\
            .withColumn("topic", lit(((self.__configuration.property("kafka.topics.output")))))

    @staticmethod
    def get_message_schema():
        return StructType([
                #StructField("timestamp",  TimestampType()),
                StructField("hardwareVersion", StringType()),
                StructField("modelDescription", StringType()),
                StructField("appVersion", StringType()),
                StructField("firmwareVersion",StringType()),
                StructField("VMStat_loadAverage", FloatType()),
                StructField("VMStat_idlePct", FloatType()),
                StructField("VMStat_iowaitPct", FloatType()),
                StructField("VMStat_systemPct", FloatType()),
                #StructField("MemoryUsage_freeKb", FloatType()),
                StructField("MemoryUsage_usedKb", FloatType()),
                StructField("MemoryUsage_totalKb", FloatType())
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
