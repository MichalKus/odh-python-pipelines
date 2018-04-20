from pyspark.sql.functions import col, split, size, when, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from common.basic_analytics.aggregations import Count, Max, Min, Avg, CompoundAggregation, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UserviceVodPlayoutProcessor(BasicAnalyticsProcessor):
    """
    Collect uservice record & vod playout metric from incoming STB requests
    """

    def _pre_process(self, stream):
        """
        Convert to appropriate timestamp type
        :param stream: input stream
        """
        tenant = self._BasicAnalyticsProcessor__configuration.property("analytics.tenant")
        filtered = stream \
            .where(~ col("x-request-id").startswith("jmeter")) \
            .where(~ col("header.user-agent").startswith("Apache")) \
            .where(col("header.user-agent") != "XAGGET") \
            .where(col("header.x-dev").isNotNull()) \
            .where(col("status").isNotNull()) \

        recording_playouts = filtered \
            .where(col("header.host") == "oboqbr.prod.{}.dmdsdp.com".format(tenant)) \
            .where(col("method") == "POST") \
            .where(col("request").contains("recording")) \
            .withColumn("recording_playout", when((col("status") >= 0) & (col("status") <= 299), "success").when(
            (col("status") >= 300) & (col("status") <= 1000), "fail").otherwise("")) \
            .where(col("recording_playout") != "") \
            .select("@timestamp", "app", "recording_playout", "status")

        vod_purchase = filtered \
            .where(col("stack") == "purchase-service-{}".format(tenant)) \
            .withColumn("vod_purchase", when((col("status") >= 0) & (col("status") <= 299), "success").when(
            (col("status") >= 300) & (col("status") <= 1000), "fail").otherwise("")) \
            .where(col("vod_purchase") != "") \
            .select("@timestamp", "app", "vod_purchase", "status")

        vod_play = filtered \
            .where(col("stack") == "session-service-{}".format(tenant)) \
            .where(col("request").contains("vod")) \
            .withColumn("vod_play", when(col("status") == 200, "success").otherwise("fail")) \
            .select("@timestamp", "app", "vod_play", "status")

        return [recording_playouts, vod_purchase, vod_play]

    def _agg_count(self, stream, type):
        """
        Aggregate uservice - he component call counts
        :param stream:
        :return:
        """
        aggregation = Count(group_fields=["app", type], aggregation_field="status",
                            aggregation_name=self._component_name)

        return stream.aggregate(aggregation)

    def _agg_unique_count(self, stream, type):
        """
        Aggregate uservice - he component call duration
        :param stream:
        :return:
        """
        aggregation = DistinctCount(group_fields=["app", type], aggregation_field="status",
                                     aggregation_name=self._component_name)

        return stream.withColumn(type, lit("all")).aggregate(aggregation)

    def _process_pipeline(self, read_stream):
        """
        Process stream via filtering and aggregating
        :param read_stream: input stream
        """

        pre_processed_streams = self._pre_process(read_stream)

        return [self._agg_count(pre_processed_streams[0], "recording_playout"),
                self._agg_count(pre_processed_streams[1], "vod_purchase"),
                self._agg_count(pre_processed_streams[2], "vod_play"),
                self._agg_unique_count(pre_processed_streams[0], "recording_playout"),
                self._agg_unique_count(pre_processed_streams[2], "vod_play")]

    @staticmethod
    def create_schema():
        """
        Define input message schema
        :return:
        """
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("x-request-id", StringType()),
            StructField("header", StructType([
                StructField("x-dev", StringType()),
                StructField("host", StringType()),
                StructField("user-agent", StringType())
            ])),
            StructField("stack", StringType()),
            StructField("app", StringType()),
            StructField("request", StringType()),
            StructField("method", StringType()),
            StructField("status", StringType())
        ])


def create_processor(configuration):
    """
    Method to create the instance of the processor
    :param configuration: dict containing configurations
    """
    return UserviceVodPlayoutProcessor(configuration, UserviceVodPlayoutProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
