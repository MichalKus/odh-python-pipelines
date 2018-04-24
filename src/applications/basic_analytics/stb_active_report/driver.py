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

        """Extract viewerID from handled record -> used in all metrics, save as protected field to ease method calls."""
        self._read_stream = read_stream \
            .withColumn("viewer_id", col("header").getItem("viewerID"))

        return [self.count_distinct_active_stb(),
                self.count_distinct_active_stb_ethernet(),
                self.count_distinct_active_stb_wifi(),
                self.count_distinct_active_stb_4k_enabled(),
                self.count_distinct_active_stb_4k_disabled(),
                self.count_distinct_active_stb_per_model(),
                self.count_distinct_active_stb_per_hw_version(),
                self.count_distinct_active_stb_per_fw_version(),
                self.count_distinct_active_stb_netflix(),
                self.count_distinct_active_stb_youtube()]

    def count_distinct_active_stb(self):
        return self._read_stream \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_ethernet(self):
        return self._read_stream \
            .withColumn("kbps", col("EthernetStats").getItem("rxKbps")) \
            .where("kbps > 0") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".ethernet"))

    def count_distinct_active_stb_wifi(self):
        return self._read_stream \
            .withColumn("kbps", col("WiFiStats").getItem("rxKbps")) \
            .where("kbps > 0") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".wifi"))

    def count_distinct_active_stb_4k_enabled(self):
        return self._read_stream \
            .withColumn("uhd", col("BCMReport").getItem("4Kcontent")) \
            .where("uhd = true") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_enabled"))

    def count_distinct_active_stb_4k_disabled(self):
        return self._read_stream \
            .withColumn("uhd", col("BCMReport").getItem("4Kcontent")) \
            .where("uhd = false") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_disabled"))

    def count_distinct_active_stb_per_model(self):
        return self._read_stream \
            .withColumn("model_name", col("header").getItem("modelName")) \
            .aggregate(DistinctCount(group_fields=["model_name"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_per_hw_version(self):
        return self._read_stream \
            .withColumn("hardware_version", col("header").getItem("hardwareVersion")) \
            .aggregate(DistinctCount(group_fields=["hardware_version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_per_fw_version(self):
        return self._read_stream \
            .withColumn("software_versions", explode(col("header").getItem("softwareVersions"))) \
            .withColumn("version", col("software_versions").getItem("version")) \
            .aggregate(DistinctCount(group_fields=["version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_netflix(self):
        return self._read_stream \
            .withColumn("provider_id", col("ApplicationsReport").getItem("provider_id")) \
            .where("provider_id = 'netflix'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".netflix"))

    def count_distinct_active_stb_youtube(self):
        return self._read_stream \
            .withColumn("provider_id", col("ApplicationsReport").getItem("provider_id")) \
            .where("provider_id = 'youtube'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".youtube"))

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
            ])),
            StructField("ApplicationsReport", StructType([
                StructField("provider_id", StringType())
            ]))
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return StbActiveReportProcessor(configuration, StbActiveReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
