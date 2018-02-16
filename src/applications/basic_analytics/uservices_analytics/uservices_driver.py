"""
The module for the driver to calculate metrics related to uServices component.
"""
from collections import namedtuple

from pyspark.sql.functions import col, when, regexp_extract, concat, lit
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, Avg
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_like
from util.kafka_pipeline_helper import start_basic_analytics_pipeline

UServiceInfo = namedtuple("uService", "app api_method")


class UServicesBasycAnalytics(BasicAnalyticsProcessor):
    """
   The processor implementation to calculate metrics related to UServices component.
   """

    __target_uservices = [
        UServiceInfo("recording-service", "bookings"),
        UServiceInfo("recording-service", "recordings"),
        UServiceInfo("purchase-service", "history"),
        UServiceInfo("purchase-service", "entitlements"),
        UServiceInfo("vod-service", "contextualvod"),
        UServiceInfo("vod-service", "detailscreen"),
        UServiceInfo("vod-service", "gridscreen"),
        UServiceInfo("discovery-service", "learn-actions"),
        UServiceInfo("discovery-service", "search"),
        UServiceInfo("discovery-service", "recommendations"),
        UServiceInfo("session-service", "channels"),
        UServiceInfo("session-service", "cpes"),
    ]

    __header_to_api_method_mapping = \
        [([uservice_info.api_method], uservice_info.api_method) for uservice_info in __target_uservices]

    def _process_pipeline(self, read_stream):
        # filter useless data
        filtered_stream = read_stream.where(
            (col("duration_ms").cast("long") != 0) &
            ~ (col("requested_uri").startswith("GET /info") | col("requested_uri").startswith("GET /prometheus"))
        )

        # filter data that contains necessary services
        uservices_predicates = map(
            lambda uservice_info: ((col("app") == uservice_info.app)
                                   &
                                   (col("header.x-original-uri").like("%/" + uservice_info.api_method + "%"))),
            self.__target_uservices)

        # join predicates into one condition
        uservices_filter = reduce(lambda col1, col2: col1.__or__(col2), uservices_predicates)

        mapped_stream = filtered_stream \
            .where(uservices_filter) \
            .withColumn("api_method",
                        custom_translate_like(col("header.x-original-uri"), self.__header_to_api_method_mapping,
                                              "undefined")) \
            .withColumn("country",
                        when(col("stack").isNotNull(),
                             regexp_extract("stack",r".*-(\w+)$", 1))
                        .otherwise("undefined")) \
            .where(col("api_method") != "undefined")

        average_duration = mapped_stream.aggregate(
            Avg(group_fields=["country", "host", "app", "app_version", "api_method"],
                aggregation_field="duration_ms",
                aggregation_name=self._component_name))

        count_by_status = mapped_stream.aggregate(
            Count(group_fields=["country", "host", "app", "app_version", "api_method", "status"],
                  aggregation_name=self._component_name))

        return [average_duration, count_by_status]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("host", StringType()),
            StructField("stack", StringType()),
            StructField("app", StringType()),
            StructField("app_version", StringType()),
            StructField("header", StructType([
                StructField("x-original-uri", StringType())
            ])),
            StructField("requested_uri", StringType()),
            StructField("duration_ms", StringType()),
            StructField("status", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return UServicesBasycAnalytics(configuration, UServicesBasycAnalytics.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
