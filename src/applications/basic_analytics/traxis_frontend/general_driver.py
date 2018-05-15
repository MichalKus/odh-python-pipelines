"""
The module for the driver to calculate metrics related to Traxis Frontend general component.
"""

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_like
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisFrontendGeneral(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Frontend general component.
    """

    def _process_pipeline(self, read_stream):
        trace_events = read_stream.where("level == 'TRACE'")
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == 'ERROR'")

        return [self.__count(info_events.union(warn_events), "info_or_warn"),
                self.__count(error_events, "error"),
                self.__host_names_unique_count(read_stream),
                self.__counts_by_level_and_hostname(read_stream),
                self.__trace_metrics(trace_events),
                self.__warn_metrics(warn_events),
                self.__info_metrics(info_events),
                self.__unclassified_successful_stream(read_stream)]

    def __count(self, events, metric_name):
        return events \
            .aggregate(Count(aggregation_name="{0}.{1}".format(self._component_name, metric_name)))

    def __host_names_unique_count(self, events):
        return events \
            .aggregate(DistinctCount(aggregation_field="hostname", aggregation_name=self._component_name))

    def __counts_by_level_and_hostname(self, events):
        return events \
            .aggregate(Count(group_fields=["level", "hostname"], aggregation_name=self._component_name))

    def __trace_metrics(self, events):
        return events \
            .withColumn("counter",
                        custom_translate_like(
                            source_field=col("message"),
                            mappings_pair=[
                                (["HTTP request received", "Referer: cdvr-bs", "<Result>success</Result>"],
                                 "vrm_success_recorded"),
                                (["HTTP request received", "Referer: cdvr-bs", "<Result>failed</Result>"],
                                 "vrm_failed_recorded"),
                                (["HTTP request", ":8080/RE/", "learnAction"], "reng_success_action"),
                                (["HTTP request received", "IsAuthorized.traxis"], "irdeto_success_request"),
                                (["HTTP request received", "User-Agent", "vod-service"], "vod_service_success"),
                                (["HTTP request received", "x-application-name: purchase-service"],
                                 "purchase_service_success"),
                                (["HTTP request received", "x-application-name: discovery-service"],
                                 "discovery_service_success"),
                                (["HTTP request received", "x-application-name: epg-packager"], "epg_success"),
                                (["HTTP request received", "x-application-name: recording-service"],
                                 "recording_service_success"),
                                (["HTTP request received", "x-application-name: session-service"],
                                 "session_service_success")
                            ],
                            default_value="unclassified")) \
            .where("counter != 'unclassified'") \
            .aggregate(Count(group_fields=["hostname", "counter"],
                             aggregation_name=self._component_name))

    def __warn_metrics(self, events):
        return events \
            .withColumn("counter",
                        custom_translate_like(
                            source_field=col("message"),
                            mappings_pair=[
                                (["Error", ":8080/RE"], "reng_error_action"),
                                (["Genre", "is not known"], "metadata_warning"),
                                (["Invalid parameter"], "invalid_parameter_warning")
                            ],
                            default_value="unclassified")) \
            .where("counter != 'unclassified'") \
            .aggregate(Count(group_fields=["hostname", "counter"],
                             aggregation_name=self._component_name))

    def __info_metrics(self, events):
        return events \
            .withColumn("counter",
                        custom_translate_like(
                            source_field=col("message"),
                            mappings_pair=[
                                (["Loading tva version", "took"], "metadata_success")
                            ],
                            default_value="unclassified")) \
            .where("counter != 'unclassified'") \
            .aggregate(Count(group_fields=["hostname", "counter"],
                             aggregation_name=self._component_name))

    def __unclassified_successful_stream(self, events):
        return events \
            .where("level in ('INFO', 'DEBUG', 'VERBOSE', 'TRACE') and lower(message) like '%succe%'") \
            .withColumn("counter", lit("unclassified_successful")) \
            .aggregate(Count(group_fields=["hostname", "counter"],
                             aggregation_name=self._component_name))

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("thread_name", StringType()),
            StructField("component", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TraxisFrontendGeneral(configuration, TraxisFrontendGeneral.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
