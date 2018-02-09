"""
Aggregate incoming stream of UsageCollectorReport.event_type messages from a Kafka topic and write to a new Kafka topic
"""
from pyspark.sql.types import StructType, StructField, StringType

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UsageCollectorEventTypeReportProcessor(BasicAnalyticsProcessor):
    """
    EOS STB driver contains logic for calculation counts of UsageCollectorReport Event Types:
    - count
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    def _process_pipeline(self, read_stream):

        return read_stream \
            .where("UsageCollectorReport_event_Type IS NOT NULL AND UsageCollectorReport_event_Type != \"\"") \
            .aggregate(Count(
            group_fields=["hardwareVersion", "firmwareVersion", "appVersion", "asVersion",
                         "UsageCollectorReport_event_Type"],
            aggregation_name=self._component_name)
        )

    @staticmethod
    def create_schema():
        """
        Create the input schema according to current processor requirements
        :return: Returns the schema
        """
        return StructType([
            StructField("timestamp", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("UsageCollectorReport_event_type", StringType())
        ])


def create_processor(configuration):
    """
    Creates stream processor object.
    :param config: Configuration object of type Configuration.
    :return: configured UsageCollectorEventTypeReportProcessor object.
    """
    return UsageCollectorEventTypeReportProcessor(configuration,
                                                  UsageCollectorEventTypeReportProcessor.create_schema(),
                                                  )


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
