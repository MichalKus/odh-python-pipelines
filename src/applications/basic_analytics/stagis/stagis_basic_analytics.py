"""
The module for the driver to calculate metrics related to Stagis component.
"""
from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from collections import namedtuple

from pyspark.sql.types import StructField, StructType, TimestampType, StringType
from pyspark.sql.functions import col

InfoMessage = namedtuple('InfoMessage', 'instance_name message')


class StagisBasicAnalytics(BasicAnalyticsProcessor):
    """
   The processor implementation to calculate metrics related to Stagis component.
   """

    def _process_pipeline(self, read_stream):
        info_messages = [
            InfoMessage('Nagra ELK Import DEX EPG', 'Imported a Nagra file successfully'),
            InfoMessage('TVA listings Ingester', "Commit succeeded for Model 'TVA listings Ingester'"),
            InfoMessage("TVA_Eredivisie Ingester", "Commit succeeded for Model 'TVA_Eredivisie Ingester'"),
            InfoMessage('DefaultsProvider', "Commit succeeded for Model 'DefaultsProvider'"),
            InfoMessage('Online Prodis VOD ingest', "Data has been successfully ingested"),
            InfoMessage('PullBasedPublisher', 'Successfully published'),
            InfoMessage('Nagra ELK Export DIM', 'Export to Nagra succeeded'),
            InfoMessage('TVA Filepublisher', 'Successfully published'),
            InfoMessage('Tva2Prodis', 'Successfully published')
        ]

        where_column = col("level").isin("ERROR", "WARN")

        for info_message in info_messages:
            where_column = where_column | \
                           (col("level") == "INFO") \
                           & (col("instance_name") == info_message.instance_name) \
                           & (col("message").like("%" + info_message.message + "%"))

        return [read_stream
                .where(where_column)
                .aggregate(Count(group_fields=["hostname", "instance_name", "level"],
                                 aggregation_name=self._component_name))
                ]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("instance_name", StringType()),
            StructField("causality_id", StringType()),
            StructField("thread_id", StringType()),
            StructField("class_name", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return StagisBasicAnalytics(configuration, StagisBasicAnalytics.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
