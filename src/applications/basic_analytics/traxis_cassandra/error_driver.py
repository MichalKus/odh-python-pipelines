import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils


class TraxisCassandraError(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        warn_events = read_stream.where("level == 'WARN'")
        error_events = read_stream.where("level == 'ERROR'")

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

        ring_status_node_errors = error_events \
            .where("message like '%Eventis.Cassandra.Service."
                   "CassandraServiceException+HostRingException%'") \
            .withColumn("host", regexp_extract("message",
                                               ".*Eventis\.Cassandra\.Service\."
                                               "CassandraServiceException\+HostRingException.*'(\S+)'.*", 1)) \
            .aggregate(Count(group_fields="host",
                             aggregation_name=self._component_name + ".ring_status_node_errors"))

        return [ring_status_node_warnings, undefined_warnings, ring_status_node_errors]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType())
        ])


def create_processor(configuration):
    return TraxisCassandraError(configuration, TraxisCassandraError.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
