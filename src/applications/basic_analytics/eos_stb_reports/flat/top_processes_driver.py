"""
Module for counting all general analytics metrics for EOS STB Top Processes
"""

from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from common.basic_analytics.aggregations import Avg
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TopProcessesEventProcessor(BasicAnalyticsProcessor):
    """
     Class that's responsible to process pipelines for STB Top Processes
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "header.ts", "@timestamp")

    def _process_pipeline(self, read_stream):
        stream = read_stream \
            .withColumn("process", explode(col("TopProcesses").getItem("processes"))) \
            .selectExpr("process.*", "`@timestamp`")

        return [stream.aggregate(Avg(group_fields="name", aggregation_field=field,
                                              aggregation_name=self._component_name))
                for field in ["rss", "fds", "threads", "vsz"]]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("ts", StringType())
            ])),
            StructField("TopProcesses", StructType([
                StructField("processes", ArrayType(
                    StructType([
                        StructField("rss", StringType()),
                        StructField("fds", StringType()),
                        StructField("name", StringType()),
                        StructField("threads", StringType()),
                        StructField("vsz", StringType())
                    ])
                ))
            ]))
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TopProcessesEventProcessor(configuration, TopProcessesEventProcessor.create_schema())


if __name__ == '__main__':
    start_basic_analytics_pipeline(create_processor)
