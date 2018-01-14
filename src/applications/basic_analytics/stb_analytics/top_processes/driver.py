import sys
import json
from common.kafka_pipeline import KafkaPipeline
from applications.basic_analytics.stb_analytics.top_processes.stb_processor import StbProcessor
from util.utils import Utils
from pyspark.sql.functions import col, collect_list, udf, split, explode, struct
from pyspark.sql.types import *

class TopProcesses(StbProcessor):
    """
    https://www.wikitechy.com/tutorials/linux/how-to-calculate-the-cpu-usage-of-a-process-by-pid-in-Linux-from-c
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration.property("analytics.componentName")
        self.kafka_output = configuration.property("kafka.topics.output")

    def _calculate_mem_usage(self, stream):
        """

        :param stream:
        :return:
        """
        def udf_mem_usage(row):
            MemoryUsage_totalKb = row[7]
            proc_rss = row[10]

            proc_mem_usage = round((float(proc_rss) / float(MemoryUsage_totalKb)) * 1000000) / 10000.0

            return proc_mem_usage

        add_mem_usage = udf(lambda row: udf_mem_usage(row), DoubleType())
        mem_usage = stream \
            .withColumn("proc_mem_usage", add_mem_usage(struct([stream[x] for x in stream.columns]))) \
            .drop('MemoryUsage_freeKb') \
            .withColumn("proc_rss", col("proc_rss").cast(DoubleType())) \
            .withColumn("proc_utime", col("proc_utime").cast(DoubleType())) \
            .withColumn("proc_stime", col("proc_stime").cast(DoubleType()))

        return mem_usage

    def _seperate_procs(self, stream):
        """
        Seperate top_procs field into each process.
        :param stream:
        :return:
        """
        def udf_split_procs(text):
            arr = text.replace('},{', '}|{').split('|')
            req_fields = ['ts', 'name', 'rss', 'utime', 'stime']
            res = []
            for elem in arr:
                proc = json.loads(elem)
                row = []
                for key in req_fields:
                    row.append(proc[key])
                res.append(row)
            return res

        split_proc = udf(lambda row: udf_split_procs(row), ArrayType(ArrayType(StringType())))
        flatten = stream \
            .withColumn("TopProcesses_processes", split_proc(col('TopProcesses_processes'))) \
            .withColumn('TopProcesses_processes', explode('TopProcesses_processes')) \
            .withColumn('proc_ts', col('TopProcesses_processes').getItem(0)) \
            .withColumn('proc_name', col('TopProcesses_processes').getItem(1)) \
            .withColumn('proc_rss', col('TopProcesses_processes').getItem(2)) \
            .withColumn('proc_utime', col('TopProcesses_processes').getItem(3)) \
            .withColumn('proc_stime', col('TopProcesses_processes').getItem(4)) \
            .drop('TopProcesses_processes')

        mem_usage = self._calculate_mem_usage(flatten)
        return mem_usage

    def _process_pipeline(self, stream):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """
        filter_udf = udf(TopProcesses.udf_filter, BooleanType())
        processed = stream \
            .groupBy('originId') \
            .agg(collect_list('timestamp'), collect_list('MemoryUsage_freeKb'), collect_list('MemoryUsage_totalKb'),
                 collect_list('TopProcesses_processes'), collect_list('hardwareVersion'), collect_list('modelDescription'),
                 collect_list('firmwareVersion'), collect_list('appVersion')) \
            .filter(filter_udf(col('collect_list(TopProcesses_processes)')))

        filled = self._fill_df(processed)
        proc_split = self._seperate_procs(filled)
        return [proc_split]

    @staticmethod
    def get_message_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("originId", StringType()),
            StructField("MemoryUsage_freeKb", StringType()),
            StructField("MemoryUsage_totalKb", StringType()),
            StructField("TopProcesses_processes", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("modelDescription", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType())
        ])

def create_processor(configuration):
    return TopProcesses(configuration, TopProcesses.get_message_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
