"""
Basic analytics driver for STB Upgrade Status via Settings report.
"""
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, IntegerType
from common.basic_analytics.aggregations import Count, Sum, Max, Min, Stddev, CompoundAggregation
from common.basic_analytics.aggregations import P01, P05, P10, P25, P50, P75, P90, P95, P99
from pyspark.sql.functions import col


class UpgradeStatusViaSettingsReportStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    Basic analytics driver for STB Upgrade Status via Settings report.
    """

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _process_pipeline(self, json_stream):
        stream = json_stream \
            .withColumn("SettingsReport_cpe_standByMode", col("SettingsReport_cpe_standByMode").cast(IntegerType())) \
            .withColumn("SettingsReport_cpe_anonymizedData", col("SettingsReport_cpe_anonymizedData").cast(IntegerType())) \

        aggregation_fields = ["SettingsReport_cpe_standByMode", "SettingsReport_cpe_anonymizedData"]
        result = []

        for field in aggregation_fields:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field}

            aggregations = [Sum(**kwargs), Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                            P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                            P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

            result.append(stream.aggregate(CompoundAggregation(aggregations=aggregations, **kwargs)))

        return result

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
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
