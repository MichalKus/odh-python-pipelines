"""Module for counting all general analytics metrics for EOS STB component"""
from pyspark.sql.types import StructField, StructType, ArrayType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, explode, from_unixtime
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class StbActiveReportProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate active STB report graphite-friendly metrics.
    (aggregations, filtering and other performance-heavy operations performed by spark)
    """

    def _process_pipeline(self, read_stream):

        """
        Extract viewerID from handled record header -> used in all metrics.
        """
        read_stream = read_stream \
            .withColumn("viewer_id", col("header").getItem("viewerID"))

        self._common_stream = read_stream

        self._ethernet_stream = read_stream \
            .withColumn("@timestamp", from_unixtime(col("EthernetStats.ts") / 1000).cast(TimestampType())) \
            .withColumn("kbps", col("EthernetStats").getItem("rxKbps"))

        self._wifi_stream = read_stream \
            .withColumn("@timestamp", from_unixtime(col("WiFiStats.ts") / 1000).cast(TimestampType())) \
            .withColumn("kbps", col("WiFiStats").getItem("rxKbps"))

        self._bcm_report_stream = read_stream \
            .withColumn("@timestamp", from_unixtime(col("BCMReport.ts") / 1000).cast(TimestampType())) \
            .withColumn("uhd", col("BCMReport").getItem("4Kcontent"))

        self._applications_report_stream = read_stream \
            .withColumn("@timestamp", from_unixtime(col("ApplicationsReport.ts") / 1000).cast(TimestampType())) \
            .withColumn("provider_id", col("ApplicationsReport").getItem("provider_id"))

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
                StructField("ts", LongType()),
                StructField("rxKbps", StringType())
            ])),
            StructField("WiFiStats", StructType([
                StructField("ts", LongType()),
                StructField("rxKbps", StringType())
            ])),
            StructField("BCMReport", StructType([
                StructField("ts", LongType()),
                StructField("4Kcontent", StringType())
            ])),
            StructField("ApplicationsReport", StructType([
                StructField("ts", LongType()),
                StructField("provider_id", StringType())
            ]))
        ])

    def count_distinct_active_stb(self):
        return self._common_stream \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_ethernet(self):
        return self._ethernet_stream \
            .where("kbps > 0") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".ethernet"))

    def count_distinct_active_stb_wifi(self):
        return self._wifi_stream \
            .where("kbps > 0") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".wifi"))

    def count_distinct_active_stb_4k_enabled(self):
        return self._bcm_report_stream \
            .where("uhd = true") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_enabled"))

    def count_distinct_active_stb_4k_disabled(self):
        return self._bcm_report_stream \
            .where("uhd = false") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".4k_disabled"))

    def count_distinct_active_stb_per_model(self):
        return self._common_stream \
            .withColumn("model_name", col("header").getItem("modelName")) \
            .aggregate(DistinctCount(group_fields=["model_name"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_per_hw_version(self):
        return self._common_stream \
            .withColumn("hardware_version", col("header").getItem("hardwareVersion")) \
            .aggregate(DistinctCount(group_fields=["hardware_version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_per_fw_version(self):
        return self._common_stream \
            .withColumn("software_versions", explode(col("header").getItem("softwareVersions"))) \
            .withColumn("version", col("software_versions").getItem("version")) \
            .aggregate(DistinctCount(group_fields=["version"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name))

    def count_distinct_active_stb_netflix(self):
        return self._applications_report_stream \
            .where("provider_id = 'netflix'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".netflix"))

    def count_distinct_active_stb_youtube(self):
        return self._applications_report_stream \
            .where("provider_id = 'youtube'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".youtube"))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return StbActiveReportProcessor(configuration, StbActiveReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
