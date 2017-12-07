import sys
import datetime
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, Avg
from util.utils import Utils
import pyspark.sql.functions as func

__author__  = "John Gregory Stockton"
__maintainer__ = "John Gregory Stockton"
__copyright__   = "Liberty Global"
__email__ = "jstockton@libertyglobal.com"
__status__ = "Pre-Prod"


logging.basicConfig(level=logging.WARN)

class ThinkAnalyticsCPU(BasicAnalyticsProcessor):
    
    def __init__(self, configuration, schema):
        
        self.__configuration = configuration
        self.__schema = schema
        self._component_name = configuration.property("analytics.componentName")
       
    

    
    def _process_pipeline(self, read_stream):

    

        uptimeAvg = read_stream.withWatermark("timestamp", "0 minutes") \
                .groupBy(window("timestamp", "15 minutes") , "json.header.hardwareVersion", ) \
                .agg(avg("json.VMStat.uptime").cast('string').alias("value"))
              
       
        idlePctAvg = read_stream.withWatermark("timestamp", "0 minutes") \
                .groupBy(window("timestamp", "15 minutes") , "json.header.hardwareVersion", ) \
                .agg(avg("json.VMStat.iowaitPct").cast('string').alias("value"))

               

        systemPct = read_stream.withWatermark("timestamp", "0 minutes") \
                .groupBy(window("timestamp", "15 minutes") , "json.header.hardwareVersion", ) \
                .agg(avg("json.VMStat.systemPct").cast('string').alias("value"))
                
        iowaitPct = read_stream.withWatermark("timestamp", "0 minutes") \
                .groupBy(window("timestamp", "15 minutes") , "json.header.hardwareVersion", ) \
                .agg(avg("json.VMStat.iowaitPct").cast('string').alias("value"))

        
        return [systemPct , idlePctAvg ,uptimeAvg, iowaitPct]





    def construct_metric_systemPct(self, dataframe):
        
        return dataframe.withColumn("metric_name", lit(("test_oboecx_pr_eosdtv_nl_prd_stb_systemPct_avg"))) 

    def construct_metric_idlePct(self, dataframe):

        return dataframe.withColumn("metric_name", lit(("test_oboecx_pr_eosdtv_nl_prd_stb_idlePct_avg"))) 

    def construct_metric_uptime(self, dataframe):

        return dataframe.withColumn("metric_name", lit(("test_oboecx_pr_eosdtv_nl_prd_stb_uptime_avg"))) 
    
    def construct_metric_iowaitPct(self, dataframe):

        return dataframe.withColumn("metric_name", lit(("test_oboecx_pr_eosdtv_nl_prd_stb_iowaitPct"))) 

    
    #Overriding
    def create(self, read_stream):
     
  
        json_stream = read_stream.select(read_stream["timestamp"].cast("timestamp").alias("timestamp"), 
                from_json(read_stream["value"].cast("string"), self.get_message_schema()).alias("json")) 
                
        dataframes = self._process_pipeline(json_stream)       

       
        
        dataframesOutput = []

        dataframesOutput.append(self.construct_metric_systemPct((dataframes[0])))
       
        dataframesOutput.append((self.construct_metric_idlePct((dataframes[1]))))
        
        dataframesOutput.append((self.construct_metric_uptime((dataframes[2]))))

        dataframesOutput.append((self.construct_metric_iowaitPct((dataframes[3]))))

        
        
        return [self.convert_to_kafka_structure(dataframe) for dataframe in dataframesOutput]
   
       

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
                StructField("header", StructType([
                StructField("ts",  TimestampType()),
                StructField("hardwareVersion", StringType()),
                StructField("modelName", StringType()),
            ])),
                StructField("VMStat", StructType([ 
                StructField("uptime", FloatType()),
                StructField("idlePct", FloatType()),
                StructField("iowaitPct", FloatType()),
                StructField("systemPct", FloatType())
                
            ])
                )])


def create_processor(configuration):
    return ThinkAnalyticsCPU(configuration, ThinkAnalyticsCPU.get_message_schema())
    

if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
