"""
Basic analytics driver for STB Upgrade Status via Settings report.
"""
from pyspark.sql.types import StructField, StructType, StringType, LongType

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UpgradeStatusViaSettingsReportStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    Basic analytics driver for STB Upgrade Status via Settings report.
    """

    def _process_pipeline(self, json_stream):
        aggregation_fields = ["SettingsReport_cpe_standByMode", "SettingsReport_cpe_anonymizedData"]

        result = []
        for field in aggregation_fields:
            result.append(
                json_stream.aggregate(
                    Count(
                        group_fields=["hardwareVersion", "firmwareVersion", "appVersion", "asVersion", field],
                        aggregation_name=self._component_name,
                    )
                )
            )

        return result

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    @staticmethod
    def create_schema():
        return StructType([
            StructField("timestamp", LongType()),
            StructField("hardwareVersion", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("SettingsReport_cpe_standByMode", StringType()),
            StructField("SettingsReport_cpe_anonymizedData", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return UpgradeStatusViaSettingsReportStbBasicAnalytics(
        configuration,
        UpgradeStatusViaSettingsReportStbBasicAnalytics.create_schema()
    )


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
