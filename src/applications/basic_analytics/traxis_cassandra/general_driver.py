"""
The module for the driver to calculate metrics related to Traxis Cassandra general component.
"""

from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_to_underlined
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisCassandraGeneral(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Cassandra general component.
    """

    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == ERROR")

        info_or_warn_count = info_events.union(warn_events) \
            .aggregate(Count(aggregation_name=self._component_name + ".info_or_warn"))

        error_count = error_events.union(warn_events) \
            .aggregate(Count(aggregation_name=self._component_name + ".error"))

        daily_starts = info_events \
            .where("message like '%Starting daily Cassandra maintenance%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".daily_starts"))

        daily_ends = info_events \
            .where("message like '%Daily Cassandra maintenance took%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".daily_ends"))

        weekly_starts = info_events \
            .where("message like '%Starting weekly Cassandra maintenance%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".weekly_starts"))

        weekly_ends = info_events \
            .where("message like '%Weekly Cassandra maintenance took%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".weekly_ends"))

        repairs = info_events \
            .where("message like '%repair%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".repairs"))

        compactings = info_events \
            .where("message like '%Compacting%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".compactings"))

        node_ups = info_events \
            .where("message like '%InetAddress /% is now UP%'") \
            .withColumn("host", regexp_extract("message", r".*InetAddress\s+/(\S+)\s+is\s+now\s+UP.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".node_ups"))

        node_downs = info_events \
            .where("message like '%InetAddress /% is now DOWN%'") \
            .withColumn("host", regexp_extract("message", r".*InetAddress\s+/(\S+)\s+is\s+now\s+DOWN.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".node_downs"))

        ring_status_node_warnings = warn_events \
            .where("message like '%Unable to determine external address "
                   "of node with internal address %'") \
            .withColumn("host", regexp_extract("message", r".*Unable\s+to\s+determine\s+external\s+address\s+of\s+"
                                                          r"node\s+with\s+internal\s+address\s+'(\S+)'.*", 1)) \
            .aggregate(Count(group_fields=["hostname", "host"],
                             aggregation_name=self._component_name + ".ring_status_node_warnings"))

        undefined_warnings = warn_events \
            .where("message not like '%Unable to determine external address "
                   "of node with internal address %'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".undefined_warnings"))

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

        successful_repairs = [read_stream.where("message like \'%{0} is fully%\'".format(message_type))
                                  .aggregate(Count(aggregation_name=self._component_name +
                                                                    convert_to_underlined(message_type)))
                              for message_type in successful_repairs_message_types]

        return [info_or_warn_count, error_count, daily_starts, daily_ends, weekly_starts, weekly_ends, repairs,
                compactings, node_ups, node_downs, ring_status_node_warnings, undefined_warnings
                ].extend(successful_repairs)

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    return TraxisCassandraGeneral(configuration, TraxisCassandraGeneral.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
