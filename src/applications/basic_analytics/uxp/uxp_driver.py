"""
This module contains code of basic UXP basic analytic spark job driver.
"""

import sys
from pyspark.sql.types import StructType, StructField, StringType, LongType

from common.basic_analytics.aggregations import Count, Avg
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from util import Utils


class UxpBAProcessor(BasicAnalyticsProcessor):
    """
    This is a driver class for UXP basic analytics spark job.
    """

    __processing_urls = ["id.virginmedia.com/oidc/authorize", "id.virginmedia.com/oidc/token",
                         "id.virginmedia.com/rest/v40/session/start", "id.virginmedia.com/oidc/token/revokeToken"]

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

    def _process_pipeline(self, uxp_stream):
        """
        Returns list with streams for aggragated fields.
        :param uxp_stream: input stream
        :return: list of processed streams
        """

        filtered_exp_stream = uxp_stream.where(uxp_stream.url.isin(self.__processing_urls))

        uxp_count_stream = filtered_exp_stream \
            .aggregate(Count(group_fields=["status code"], aggregation_name=self._component_name))

        uxp_avg_response_time_stream = filtered_exp_stream \
            .aggregate(Avg(group_fields=["responseTime"], aggregation_name=self._component_name))

        return [uxp_count_stream, uxp_avg_response_time_stream]


def create_processor(config):
    """
    Creates stream processor object.
    :param config: Configuration obejct of type Configuration.
    :return: configured Uxp object.
    """

    return UxpBAProcessor(config, UxpBAProcessor.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
