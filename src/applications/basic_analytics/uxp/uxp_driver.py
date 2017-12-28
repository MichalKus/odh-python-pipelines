"""
This module contains code of basic UXP basic analytic spark job driver.
"""

from pyspark.sql.functions import from_unixtime, col, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from common.basic_analytics.aggregations import Count, Avg
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import custom_translate_like
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class UxpBasicAnalyticsProcessor(BasicAnalyticsProcessor):
    """
    This is a driver class for UXP basic analytics spark job.
    """

    __processing_urls = ["id.virginmedia.com/oidc/authorize", "id.virginmedia.com/oidc/token",
                         "id.virginmedia.com/rest/v40/session/start", "id.virginmedia.com/oidc/token/revokeToken"]

    __url_mapping = [(["id.virginmedia.com/oidc/authorize"], "authorize"),
                     (["id.virginmedia.com/oidc/token"], "token"),
                     (["id.virginmedia.com/rest/v40/session/start"], "session_start"),
                     (["id.virginmedia.com/oidc/token/revokeToken"], "revokeToken")]

    @staticmethod
    def create_schema():
        """
        Returns input event schema.
        :return:  StructType object.
        """

        return StructType([
            StructField("url", StringType()),
            StructField("status code", StringType()),
            StructField("responseTime", StringType()),
            StructField("time", LongType())
        ])

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("time") / 1000).cast(TimestampType()))

    def _process_pipeline(self, uxp_stream):
        """
        Returns list with streams for aggragated fields.
        :param uxp_stream: input stream
        :return: list of processed streams
        """

        filtered_exp_stream = uxp_stream \
            .where(uxp_stream.url.isin(self.__processing_urls)) \
            .select(custom_translate_like(col("url"), self.__url_mapping, lit("undefined")).alias("action"),
                    col("status code"), col("responseTime"), col("@timestamp"))

        uxp_count_stream = filtered_exp_stream \
            .aggregate(Count(group_fields=["action", "status code"], aggregation_name=self._component_name))

        uxp_avg_response_time_stream = filtered_exp_stream \
            .aggregate(Avg(aggregation_field="responseTime", group_fields=["action"],
                           aggregation_name=self._component_name))

        return [uxp_count_stream, uxp_avg_response_time_stream]


def create_processor(config):
    """
    Creates stream processor object.
    :param config: Configuration obejct of type Configuration.
    :return: configured Uxp object.
    """

    return UxpBasicAnalyticsProcessor(config, UxpBasicAnalyticsProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
