"""
The module for the driver to calculate metrics related to Traxis Backend error component.
"""

from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisBackendError(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Backend error component.
    """

    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == 'ERROR'")

        return [self.__info_or_warn_count(info_events, warn_events),
                self.__error_count(error_events),
                self.__tva_ingest_error(warn_events),
                self.__customer_provisioning_error(warn_events),
                self.__undefined_warnings(warn_events),
                self.__cassandra_errors(error_events),
                self.__undefined_errors(error_events),
                self.__total_available_hosts(read_stream),
                self.__info_or_warn_count_per_host_names(info_events, warn_events),
                self.__error_count_per_host_names(error_events)]

    def __info_or_warn_count(self, info_events, warn_events):
        return info_events.union(warn_events) \
            .aggregate(Count(aggregation_name=self._component_name + ".info_or_warn"))

    def __error_count(self, error_events):
        return error_events \
            .aggregate(Count(aggregation_name=self._component_name + ".error"))

    def __tva_ingest_error(self, warn_events):
        return warn_events \
            .where("message like '%One or more validation errors detected during tva ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_ingest_error"))

    def __customer_provisioning_error(self, warn_events):
        return warn_events \
            .where("message like '%Unable to use alias%because alias is already used by%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_error"))

    def __undefined_warnings(self, warn_events):
        return warn_events.where(
            "message not like '%Unable to use alias%because alias is already used by%' and "
            "message not like '%One or more validation errors detected during tva ingest%'"
        ).aggregate(Count(group_fields=["hostname"],
                          aggregation_name=self._component_name + ".undefined_warnings"))

    def __cassandra_errors(self, error_events):
        return error_events \
            .where("message like '%Exception with cassandra node%'") \
            .withColumn("host", regexp_extract("message",
                                               r".*Exception\s+with\s+cassandra\s+node\s+\'([\d\.]+).*", 1)
                        ) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".cassandra_errors"))

    def __undefined_errors(self, error_events):
        return error_events \
            .where("message not like '%Exception with cassandra node%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".undefined_errors"))

    def __total_available_hosts(self, read_stream):
        return read_stream \
            .aggregate(DistinctCount(aggregation_field="hostname",
                                     aggregation_name=self._component_name))

    def __info_or_warn_count_per_host_names(self, info_events, warn_events):
        return info_events.union(warn_events) \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".info_or_warn_event"))

    def __error_count_per_host_names(self, error_events):
        return error_events \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".error_event"))

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TraxisBackendError(configuration, TraxisBackendError.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
