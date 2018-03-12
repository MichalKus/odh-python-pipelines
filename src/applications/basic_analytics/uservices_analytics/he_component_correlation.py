from pyspark.sql.functions import col, split, size, when, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from common.basic_analytics.aggregations import Count, Max, Min, Avg, CompoundAggregation
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UserviceHeComponentProcessor(BasicAnalyticsProcessor):
    """
    Collect uservice inbound & outbound requests metric which are tied with HE components
    """

    def _pre_process(self, stream):
        """
        Convert to appropriate timestamp type
        :param data_stream: input stream
        """
        filtered = stream \
            .where((col("x-request-id").isNotNull()) | (col("header.x-request-id").isNotNull())) \
            .withColumn("tenant", split(col("stack"), "-")) \
            .withColumn("tenant", col("tenant").getItem(size(col("tenant")) - 1))

        uservice2component_count = filtered \
            .where((col("message").startswith("Attempting"))
                   | (col("message").startswith("Successfully"))
                   | (col("message").startswith("Failed to call"))) \
            .withColumn("calls", when(col("message").startswith("Attempting"), "attempts")
                        .when(col("message").startswith("Successfully"), "success")
                        .when(col("message").startswith("Failed to call"), "failures")) \
            .withColumn("requests", lit(1)) \
            .withColumn("dest", split(col("message"), "/").getItem(3)) \
            .select("@timestamp", "tenant", "app", "dest", "calls", "requests")

        uservice2component_duration = filtered \
            .where(col("http.useragent").contains("-service") & (col("http.duration") > 0)) \
            .withColumn("dest", split(col("http.request"), "/").getItem(1)) \
            .withColumn("app", split(col("http.useragent"), "/").getItem(0)) \
            .withColumn("duration_ms", col("http.duration") * 1000) \
            .select("@timestamp", "tenant", "app", "dest", "duration_ms", "host")

        end2end = filtered \
            .where((col("duration_ms") > 0) & (col("request") != "/info")) \
            .withColumn("status", concat(split(col("status"), "").getItem(0), lit("xx"))) \
            .select("@timestamp", "tenant", "app", "request", "duration_ms", "status")

        return [uservice2component_count, uservice2component_duration, end2end]

    def _agg_uservice2component_count(self, stream):
        """
        Aggregate uservice - he component call counts
        :param stream:
        :return:
        """
        aggregation = Count(group_fields=["tenant", "app", "dest", "calls"], aggregation_field="requests",
                            aggregation_name=self._component_name)

        return stream.aggregate(aggregation)

    def _agg_uservice2component_duration(self, stream):
        """
        Aggregate uservice - he component call duration
        :param stream:
        :return:
        """
        kwargs = {'aggregation_field': "duration_ms"}

        aggregations = [Max(**kwargs), Min(**kwargs), Avg(**kwargs)]

        return stream.aggregate(CompoundAggregation(aggregations=aggregations, aggregation_name=self._component_name,
                                                    group_fields=["tenant", "app", "dest", "host"]))

    def _agg_end2end(self, stream):
        """
        Aggregate end to end calls from STB - uservice
        :param stream:
        :return:
        """
        kwargs = {'aggregation_field': "duration_ms"}

        aggregations = [Max(**kwargs), Min(**kwargs), Avg(**kwargs)]

        return stream.aggregate(CompoundAggregation(aggregations=aggregations, aggregation_name=self._component_name,
                                                    group_fields=["tenant", "app", "status"]))

    def _process_pipeline(self, read_stream):
        """
        Process stream via filtering and aggregating
        :param read_stream: input stream
        """

        pre_processed_streams = self._pre_process(read_stream)

        return [self._agg_uservice2component_count(pre_processed_streams[0]),
                self._agg_uservice2component_duration(pre_processed_streams[1]),
                self._agg_end2end(pre_processed_streams[2])]

    @staticmethod
    def create_schema():
        """
        Define input message schema
        :return:
        """
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("stack", StringType()),
            StructField("app", StringType()),
            StructField("x-request-id", StringType()),
            StructField("header", StructType([
                StructField("x-request-id", StringType())
            ])),
            StructField("host", StringType()),
            StructField("message", StringType()),
            StructField("request", StringType()),
            StructField("status", StringType()),
            StructField("duration_ms", DoubleType()),
            StructField("http", StructType([
                StructField("request", StringType()),
                StructField("useragent", StringType()),
                StructField("duration", DoubleType())
            ]))
        ])


def create_processor(configuration):
    """
    Method to create the instance of the processor
    :param configuration: dict containing configurations
    """
    return UserviceHeComponentProcessor(configuration, UserviceHeComponentProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
