"""
The module for the driver to calculate metrics related to Traxis Cassandra general component.
"""

from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_to_underlined, custom_translate_regex, custom_translate_like
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisCassandraGeneral(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Cassandra general component.
    """

    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == 'ERROR'")

        return [self.__count(info_events.union(warn_events), "info_or_warn"),
                self.__count(error_events, "error"),
                self.__daily_starts(info_events),
                self.__daily_ends(info_events),
                self.__weekly_starts(info_events),
                self.__weekly_ends(info_events),
                self.__repairs(info_events),
                self.__compactings(info_events),
                self.__node_ups(info_events),
                self.__node_downs(info_events),
                self.__ring_status_node_warnings(warn_events),
                self.__undefined_warnings(warn_events),
                self.__successful_repairs(read_stream),
                self.__unreachable_nodes(error_events),
                self.__reachable_nodes(read_stream),
                self.__memtable_flush(read_stream),
                self.__total_available_hosts(read_stream),
                self.__log_levels(read_stream),
                self.__memory_flushing(read_stream)]

    def __count(self, events, metric_name):
        return events \
            .aggregate(Count(aggregation_name="{0}.{1}".format(self._component_name, metric_name)))

    def __daily_starts(self, events):
        return events \
            .where("message like '%Starting daily Cassandra maintenance%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".daily_starts"))

    def __daily_ends(self, events):
        return events \
            .where("message like '%Daily Cassandra maintenance took%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".daily_ends"))

    def __weekly_starts(self, events):
        return events \
            .where("message like '%Starting weekly Cassandra maintenance%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".weekly_starts"))

    def __weekly_ends(self, events):
        return events \
            .where("message like '%Weekly Cassandra maintenance took%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".weekly_ends"))

    def __repairs(self, events):
        return events \
            .where("message like '%repair%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".repairs"))

    def __compactings(self, events):
        return events \
            .where("message like '%Compacting%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".compactings"))

    def __node_ups(self, events):
        return events \
            .where("message like '%InetAddress /% is now UP%'") \
            .withColumn("host", regexp_extract("message", r".*InetAddress\s+/(\S+)\s+is\s+now\s+UP.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".node_ups"))

    def __node_downs(self, events):
        return events \
            .where("message like '%InetAddress /% is now DOWN%'") \
            .withColumn("host", regexp_extract("message", r".*InetAddress\s+/(\S+)\s+is\s+now\s+DOWN.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".node_downs"))

    def __ring_status_node_warnings(self, events):
        return events \
            .where("message like '%Unable to determine external address "
                   "of node with internal address %'") \
            .withColumn("host", regexp_extract("message", r".*Unable\s+to\s+determine\s+external\s+address\s+of\s+"
                                                          r"node\s+with\s+internal\s+address\s+'(\S+)'.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".ring_status_node_warnings"))

    def __undefined_warnings(self, events):
        return events \
            .where("message not like '%Unable to determine external address "
                   "of node with internal address %'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".undefined_warnings"))

    def __successful_repairs(self, events):
        successful_repairs_message_types = [
            "PreOrderProducts",
            "PromotionRuleEvaluations",
            "Logs",
            "ProductsPriceHistory",
            "NotificationGroups",
            "PreOrderPurchases",
            "RecordedContentGroups",
            "TraxisRtspTraceData",
            "CustomerPromotionRules",
            "RecordedTitles",
            "RecordedContents",
            "Promotions",
            "RecordingInstructions",
            "Customers",
            "TraxisWebTraceData",
            "Configuration",
            "NotificationProfiles",
            "TaskStatistics"
        ]

        mapping = {"{0} is fully".format(message_type): convert_to_underlined(message_type)
                   for message_type in successful_repairs_message_types}

        return events \
            .withColumn("type", custom_translate_regex(source_field=col("message"), mapping=mapping,
                                                       default_value="unclassified")).where("type != 'unclassified'") \
            .aggregate(Count(group_fields=["type"], aggregation_name=self._component_name))

    def __unreachable_nodes(self, events):
        return events \
            .where("message like '%Node % is unreachable%'") \
            .aggregate(DistinctCount(group_fields=["hostname"], aggregation_field="hostname",
                                     aggregation_name=self._component_name + ".unreachable_nodes"))

    def __reachable_nodes(self, events):
        return events \
            .where("not(message like '%Node % is unreachable%')") \
            .aggregate(DistinctCount(group_fields=["hostname"], aggregation_field="hostname",
                                     aggregation_name=self._component_name + ".reachable_nodes"))

    def __memtable_flush(self, events):
        return events \
            .where("message like '%Heap is%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".memtable_flush"))

    def __total_available_hosts(self, events):
        return events \
            .aggregate(DistinctCount(aggregation_field="hostname",
                                     aggregation_name=self._component_name))

    def __log_levels(self, events):
        return events \
            .aggregate(Count(group_fields=["level", "hostname"],
                             aggregation_name=self._component_name + ".info_warn"))

    def __memory_flushing(self, events):
        return events \
            .where("message like '%Flushing%'") \
            .withColumn("column_family", custom_translate_like(
                source_field=col("message"),
                mappings_pair=[(["Channels"], "channels"),
                               (["Titles"], "titles"),
                               (["Groups"], "groups")],
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
    """
    Creating processor method to calculate metrics related to Traxis Cassandra general component
    :param configuration: job configuration
    :return: new processor object
    """
    return TraxisCassandraGeneral(configuration, TraxisCassandraGeneral.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
