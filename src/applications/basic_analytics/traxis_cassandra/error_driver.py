"""
The module for the driver to calculate metrics related to Traxis Cassandra error component.
"""

from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_regex
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisCassandraError(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Cassandra error component.
    """

    def _process_pipeline(self, read_stream):
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == 'ERROR'")

        return [self.__info_or_warn_count(read_stream),
                self.__error_count(error_events),
                self.__ring_status_node_warnings(warn_events),
                self.__undefined_warnings(warn_events),
                self.__ring_status_node_errors(error_events),
                self.__success_logs(read_stream),
                self.__failure_logs(read_stream),
                self.__memory_flushing(read_stream)]

    def __info_or_warn_count(self, read_stream):
        return read_stream \
            .where("level == 'INFO' or level == 'WARN'") \
            .aggregate(Count(aggregation_name=self._component_name + ".info_or_warn"))

    def __error_count(self, error_events):
        return error_events \
            .aggregate(Count(aggregation_name=self._component_name + ".error"))

    def __ring_status_node_warnings(self, warn_events):
        return warn_events \
            .where("message like '%Unable to determine external address "
                   "of node with internal address %'") \
            .withColumn("host", regexp_extract("message", r".*Unable\s+to\s+determine\s+external\s+address\s+of\s+"
                                                          r"node\s+with\s+internal\s+address\s+'(\S+)'.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".ring_status_node_warnings"))

    def __undefined_warnings(self, warn_events):
        return warn_events \
            .where("message not like '%Unable to determine external address "
                   "of node with internal address %'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".undefined_warnings"))

    def __ring_status_node_errors(self, error_events):
        return error_events \
            .where("message like '%Eventis.Cassandra.Service."
                   "CassandraServiceException+HostRingException%'") \
            .withColumn("host", regexp_extract("message", r".*Eventis\.Cassandra\.Service\.CassandraServiceException\+"
                                                          r"HostRingException.*'(\S+)'.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".ring_status_node_errors"))

    def __success_logs(self, events):
        return events \
            .where("level == 'INFO' or level =='WARN'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".success_logs"))

    def __failure_logs(self, events):
        return events \
            .where("level == 'ERROR'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + "failure_logs"))

    def __memory_flushing(self, events):
        return events \
            .where("message like '%Flushing%'") \
            .withColumn("column_family", custom_translate_regex(
                source_field=col("message"),
                mapping={r".*Channels.*": "channels",
                         r".*Titles.*": "titles",
                         r".*Groups.*": "groups"},
                default_value="unclassified")) \
            .where("column_family != 'unclassified'") \
            .aggregate(Count(group_fields=["column_family"],
                             aggregation_name=self._component_name + ".memory_flushing"))

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
    return TraxisCassandraError(configuration, TraxisCassandraError.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
