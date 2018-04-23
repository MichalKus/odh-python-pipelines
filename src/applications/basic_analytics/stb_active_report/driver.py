"""Module for counting all general analytics metrics for EOS STB component"""
from pyspark.sql.types import StructField, StructType, ArrayType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, explode
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class StbActiveReportProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate active STB report graphite-friendly metrics.
    (aggregations, filtering and other performance-heavy operations performed by spark)
    """

    def _process_pipeline(self, read_stream):

        """Extract viewerID from handled record -> used in all metrics."""
        read_stream = read_stream \
            .withColumn("viewer_id", col("header").getItem("viewerID"))

        count_distinct_active_stb = read_stream \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb"))

        count_distinct_active_stb_ethernet = read_stream \
            .withColumn("kbps", col("EthernetStats").getItem("rxKbps")) \
            .where("kbps > 0") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_ethernet"))

        count_distinct_active_stb_wifi = read_stream \
            .withColumn("kbps", col("WiFiStats").getItem("rxKbps")) \
            .where("kbps > 0") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_wifi"))

        count_distinct_active_stb_4k_enabled = read_stream \
            .withColumn("uhd", col("BCMReport").getItem("4Kcontent")) \
            .where("uhd = true") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_4k_enabled"))

        count_distinct_active_stb_4k_disabled = read_stream \
            .withColumn("uhd", col("BCMReport").getItem("4Kcontent")) \
            .where("uhd = false") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_4k_disabled"))

        count_distinct_active_stb_per_model = read_stream \
            .withColumn("model_name", col("header").getItem("modelName")) \
            .aggregate(DistinctCount(group_fields=["model_name"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_per_model"))

        count_distinct_active_stb_per_hw_version = read_stream \
            .withColumn("hardware_version", col("header").getItem("hardwareVersion")) \
            .aggregate(DistinctCount(group_fields=["hardware_version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_per_hw_version"))

        count_distinct_active_stb_per_fw_version = read_stream \
            .withColumn("software_versions", explode(col("header").getItem("softwareVersions"))) \
            .withColumn("version", col("software_versions").getItem("version")) \
            .aggregate(DistinctCount(group_fields=["version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_stb_per_fw_version"))

        return [count_distinct_active_stb, count_distinct_active_stb_ethernet, count_distinct_active_stb_wifi,
                count_distinct_active_stb_4k_enabled, count_distinct_active_stb_4k_disabled,
                count_distinct_active_stb_per_model, count_distinct_active_stb_per_hw_version,
                count_distinct_active_stb_per_fw_version]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType()),
                StructField("modelName", StringType()),
                StructField("hardwareVersion", StringType()),
                StructField("softwareVersions", ArrayType(
                    StructType([
                        StructField("version", StringType())
                    ])
                ))
            ])),
            StructField("EthernetStats", StructType([
                StructField("rxKbps", StringType())
            ])),
            StructField("WiFiStats", StructType([
                StructField("rxKbps", StringType())
            ])),
            StructField("BCMReport", StructType([
                StructField("4Kcontent", StringType())
            ]))
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return StbActiveReportProcessor(configuration, StbActiveReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
