"""
The module for the driver to calculate metrics related to Stagis component.
"""
from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from collections import namedtuple

from pyspark.sql.types import StructField, StructType, TimestampType, StringType
from pyspark.sql.functions import col, lower, lit
from common.spark_utils.custom_functions import custom_translate_like

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

        by_specific_info_messages = read_stream.where(where_column)\
            .aggregate(Count(group_fields=["hostname", "instance_name", "level"],
                             aggregation_name=self._component_name))

        count_by_classname = read_stream.aggregate(Count(group_fields=["class_name"],
                                                      aggregation_name=self._component_name))
        count_by_hostname = read_stream.aggregate(Count(group_fields=["hostname"],
                                            aggregation_name=self._component_name))

        count_by_hostname_and_common_levels = read_stream\
            .where((col("level") == "INFO") | (col("level") == "WARN") | (col("level") == "ERROR"))\
            .aggregate(Count(group_fields=["hostname", "level"], aggregation_name=self._component_name))

        count_by_hostname_and_other_levels = read_stream \
            .where((col("level") != "INFO") & (col("level") != "WARN") & (col("level") != "ERROR")) \
            .withColumn("level", lit("other"))\
            .aggregate(Count(group_fields=["hostname", "level"], aggregation_name=self._component_name))

        count_by_instance_and_level = read_stream.aggregate(Count(group_fields=["instance_name", "level"],
                                                            aggregation_name=self._component_name))
        count_by_instance = read_stream.aggregate(Count(group_fields=["instance_name"],
                                                        aggregation_name=self._component_name))

        other_metrics = read_stream \
            .where((col("message").like("%saved%") & (col("level") == "INFO"))
                   | (col("message").like("%cannot download% TVA%") & (col("level") == "ERROR"))
                   | (col("message").like("Successfully published. 'Full'%"))
                   | (col("message").like("Successfully published. 'Delta'%"))
                   | (col("message").like("Updating%") & (col("level") == "INFO") & (lower(col("class_name")) == "p"))
                   | (col("message").like("Inserting%") & (col("level") == "INFO") & (lower(col("class_name")) == "p"))
                   | (col("message").like("'PRODIS%"))
                   | (col("message").like("%traxis% succeeded%"))
                   | (col("message").like("%Subscriber added%"))
                   | (col("message").like("%Subscriber removed%"))
                   ) \
            .withColumn("message_type",
                        custom_translate_like(
                            source_field=col("message"),
                            mappings_pair=[
                                (["saved"], "catalog_ingestion_success"),
                                (["cannot download", "TVA"], "catalog_ingestion_failure"),
                                (["Successfully published. 'Full'"], "full_feed_published"),
                                (["Successfully published. 'Delta'"], "delta_feeds_published"),
                                (["Updating"], "catalog_update"),
                                (["Inserting"], "catalog_insert"),
                                (["'PRODIS"], "prodis_delta_pull_requests"),
                                (["traxis", "succeeded"], "subscription_manager_traxis"),
                                (["Subscriber added"], "subscriber_addition"),
                                (["Subscriber removed"], "subscriber_removal")
                            ],
                            default_value="unclassified")) \
            .aggregate(Count(group_fields="message_type", aggregation_name=self._component_name))

        return [by_specific_info_messages, count_by_classname, count_by_hostname, count_by_hostname_and_common_levels,
                count_by_hostname_and_other_levels, count_by_instance_and_level, count_by_instance, other_metrics]

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
