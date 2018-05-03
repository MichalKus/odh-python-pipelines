"""
Basic analytics driver for STB header report.
"""
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from common.basic_analytics.aggregations import DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class HeaderStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to STB header report.
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "header.ts", "@timestamp")

    def _process_pipeline(self, read_stream):

        stream = read_stream.withColumn("viewer_id", col("header").getItem("viewerID"))

        distinct_count_per_software_version_stream = stream \
            .withColumn("model_name", col("header").getItem("modelName")) \
            .aggregate(DistinctCount(group_fields=["model_name"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

        distinct_count_per_hardware_version_stream = stream \
            .withColumn("hardware_version", col("header").getItem("hardwareVersion")) \
            .aggregate(DistinctCount(group_fields=["hardware_version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

        distinct_count_per_model_stream = stream \
            .withColumn("software_versions", explode(col("header").getItem("softwareVersions"))) \
            .withColumn("version", col("software_versions").getItem("version")) \
            .aggregate(DistinctCount(group_fields=["version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

        return [distinct_count_per_software_version_stream,
                distinct_count_per_hardware_version_stream,
                distinct_count_per_model_stream]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("header", StructType([
                StructField("viewerID", StringType()),
                StructField("ts", StringType()),
                StructField("modelName", StringType()),
                StructField("hardwareVersion", StringType()),
                StructField("softwareVersions", ArrayType(
                    StructType([
                        StructField("version", StringType())])
                    ))
                ]))
            ])



def create_processor(configuration):
    """Method to create the instance of the processor"""
    return HeaderStbBasicAnalytics(configuration, HeaderStbBasicAnalytics.create_schema())


if __name__ == '__main__':
    start_basic_analytics_pipeline(create_processor)
