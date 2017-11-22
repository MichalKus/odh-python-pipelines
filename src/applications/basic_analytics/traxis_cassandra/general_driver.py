import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils


class TraxisCassandraGeneral(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")

        daily_starts = info_events \
            .where("message like '%Starting daily Cassandra maintenance%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".daily_starts"))

        daily_ends = info_events \
            .where("message like '%Daily Cassandra maintenance took%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".daily_ends"))

        weekly_starts = info_events \
            .where("message like '%Starting weekly Cassandra maintenance%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".weekly_starts"))

        weekly_ends = info_events \
            .where("message like '%Weekly Cassandra maintenance took%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".weekly_ends"))

        repairs = info_events \
            .where("message like '%repair%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".repairs"))

        compactings = info_events \
            .where("message like '%Compacting%'") \
            .aggregate(Count(aggregation_name=self._component_name + ".compactings"))

        node_ups = info_events \
            .where("message like '%InetAddress /% is now UP%'") \
            .withColumn("host", regexp_extract("message", ".*InetAddress\s+/(\S+)\s+is\s+now\s+UP.*", 1)) \
            .aggregate(Count(group_fields="host", aggregation_name=self._component_name + ".node_ups"))

        node_downs = info_events \
            .where("message like '%InetAddress /% is now DOWN%'") \
            .withColumn("host", regexp_extract("message", ".*InetAddress\s+/(\S+)\s+is\s+now\s+DOWN.*", 1)) \
            .aggregate(Count(group_fields="host", aggregation_name=self._component_name + ".node_downs"))

        ring_status_node_warnings = warn_events \
            .where("message like '%Unable to determine external address "
                   "of node with internal address %'") \
            .withColumn("host", regexp_extract("message",
                                               ".*Unable\s+to\s+determine\s+external\s+address\s+"
                                               "of\s+node\s+with\s+internal\s+address\s+'(\S+)'.*", 1)) \
            .aggregate(Count(group_fields="host",
                             aggregation_name=self._component_name + ".ring_status_node_warnings"))

        undefined_warnings = warn_events \
            .where("message not like '%Unable to determine external address "
                   "of node with internal address %'") \
            .aggregate(Count(aggregation_name=self._component_name + ".undefined_warnings"))

        return [daily_starts, daily_ends, weekly_starts, weekly_ends, repairs,
                compactings, node_ups, node_downs, ring_status_node_warnings,
                undefined_warnings]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType())
        ])


def create_processor(configuration):
    return TraxisCassandraGeneral(configuration, TraxisCassandraGeneral.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
