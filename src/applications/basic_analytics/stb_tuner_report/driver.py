"""Module for counting all general analytics metrics for EOS STB component"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import DistinctCount, Avg, Max
from pyspark.sql.functions import col, from_unixtime
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class StbTunerReportProcessor(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate tuner report graphite-friendly metrics.
    (aggregations, filtering and other performance-heavy operations performed by spark)
    """

    def _process_pipeline(self, read_stream):

        """
        Extract viewerID handled record header -> frequently used in metrics.
        """
        read_stream = read_stream \
            .withColumn("viewer_id", col("header").getItem("viewerID"))

        self._tuner_report_stream = read_stream \
            .withColumn("@timestamp", from_unixtime(col("TunerReport.ts") / 1000).cast(TimestampType()))

        return [self.count_avg_snr(),
                self.count_avg_signal_level_dbm(),
                self.count_max_snr_by_viewer_id(),
                self.count_distinct_stb_by_report_index(),
                self.count_avg_frequency_stb_by_report_index(),
                self.count_max_correcteds_by_viewer_id(),
                self.count_max_erroreds_by_viewer_id(),
                self.count_max_pre_ecber_by_viewer_id(),
                self.count_max_post_ecber_by_viewer_id()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("header", StructType([
                StructField("viewerID", StringType()),
            ])),
            StructField("TunerReport", StructType([
                StructField("ts", LongType()),
                StructField("index", StringType()),
                StructField("signalLevel", StringType()),
                StructField("SNR", StringType()),
                StructField("locked", StringType()),
                StructField("correcteds", StringType()),
                StructField("erroreds", StringType()),
                StructField("preECBER", StringType()),
                StructField("postECBER", StringType()),
                StructField("frequency", StringType()),
            ]))
        ])

    def count_avg_snr(self):
        return self._tuner_report_stream \
            .withColumn("snr", col("TunerReport").getItem("SNR")) \
            .aggregate(Avg(aggregation_field="snr",
                           aggregation_name=self._component_name))

    def count_avg_signal_level_dbm(self):
        return self._tuner_report_stream \
            .withColumn("signal_level", col("TunerReport").getItem("signalLevel")) \
            .aggregate(Avg(aggregation_field="signal_level",
                           aggregation_name=self._component_name + ".dbm"))

    def count_max_snr_by_viewer_id(self):
        return self._tuner_report_stream \
            .withColumn("snr", col("TunerReport").getItem("SNR")) \
            .aggregate(Max(group_fields=["viewer_id"],
                           aggregation_field="snr",
                           aggregation_name=self._component_name))

    def count_distinct_stb_by_report_index(self):
        return self._tuner_report_stream \
            .withColumn("index", col("TunerReport").getItem("index")) \
            .withColumn("locked", col("TunerReport").getItem("locked")) \
            .where("locked = true") \
            .aggregate(DistinctCount(group_fields=["index"],
                                     aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".locked"))

    def count_avg_frequency_stb_by_report_index(self):
        return self._tuner_report_stream \
            .withColumn("index", col("TunerReport").getItem("index")) \
            .withColumn("frequency", col("TunerReport").getItem("frequency")) \
            .withColumn("locked", col("TunerReport").getItem("locked")) \
            .where("locked = true") \
            .aggregate(Avg(group_fields=["index"],
                           aggregation_field="frequency",
                           aggregation_name=self._component_name + ".locked"))

    def count_max_correcteds_by_viewer_id(self):
        return self._tuner_report_stream \
            .withColumn("correcteds", col("TunerReport").getItem("correcteds")) \
            .aggregate(Max(group_fields=["viewer_id"],
                           aggregation_field="correcteds",
                           aggregation_name=self._component_name))

    def count_max_erroreds_by_viewer_id(self):
        return self._tuner_report_stream \
            .withColumn("erroreds", col("TunerReport").getItem("erroreds")) \
            .aggregate(Max(group_fields=["viewer_id"],
                           aggregation_field="erroreds",
                           aggregation_name=self._component_name))

    def count_max_pre_ecber_by_viewer_id(self):
        return self._tuner_report_stream \
            .withColumn("pre_ecber", col("TunerReport").getItem("preECBER")) \
            .where("pre_ecber != 0") \
            .aggregate(Max(group_fields=["viewer_id"],
                           aggregation_field="pre_ecber",
                           aggregation_name=self._component_name + ".pre_ecber_not_zero"))

    def count_max_post_ecber_by_viewer_id(self):
        return self._tuner_report_stream \
            .withColumn("pre_ecber", col("TunerReport").getItem("preECBER")) \
            .withColumn("post_ecber", col("TunerReport").getItem("postECBER")) \
            .where("pre_ecber != 0") \
            .aggregate(Max(group_fields=["viewer_id"],
                           aggregation_field="post_ecber",
                           aggregation_name=self._component_name + ".post_ecber_not_zero"))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return StbTunerReportProcessor(configuration, StbTunerReportProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
