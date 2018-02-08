"""
Basic analytics driver for STB Tempetarure values for all hardware components.
"""
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.types import StructField, StructType, StringType
from common.basic_analytics.aggregations import Count, Max, Min, Stddev, CompoundAggregation
from common.basic_analytics.aggregations import P01, P05, P10, P25, P50, P75, P90, P95, P99
from pyspark.sql.functions import lit, col, from_json, when


class TemperatureStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    Basic analytics driver for STB Tempetarure values for all hardware components.
    """

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    def _process_pipeline(self, json_stream):
        schema = StructType([
            StructField("TUNER", StringType()),
            StructField("BOARD", StringType()),
            StructField("WIFI", StringType()),
            StructField("CPU", StringType()),
            StructField("HDD", StringType())
        ])

        stream = json_stream \
            .withColumn("jsonHW", from_json(col("TemperatureReport_value"), schema).alias("jsonHW")) \
            .withColumn("TUNER", when(col("jsonHW.TUNER") == "-274", None).otherwise(col("jsonHW.TUNER"))) \
            .withColumn("BOARD", when(col("jsonHW.BOARD") == "-274", None).otherwise(col("jsonHW.BOARD"))) \
            .withColumn("WIFI", when(col("jsonHW.WIFI") == "-274", None).otherwise(col("jsonHW.WIFI"))) \
            .withColumn("CPU", when(col("jsonHW.CPU") == "-274", None).otherwise(col("jsonHW.CPU"))) \
            .withColumn("HDD", when(col("jsonHW.HDD") == "-274", None).otherwise(col("jsonHW.HDD"))) \
            .drop("jsonHW") \
            .drop("TemperatureReport_value")

        aggregation_fields = ["TUNER", "BOARD", "WIFI", "CPU", "HDD"]
        result = []

        for field in aggregation_fields:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field}

            aggregations = [Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                            P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                            P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

            result.append(stream.aggregate(CompoundAggregation(aggregations=aggregations, **kwargs)))

        return result

    @staticmethod
    def create_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("TemperatureReport_value", StringType())
        ])

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    def _post_processing_pipeline(self, dataframe):
        return dataframe \
            .filter(col("value").isNotNull()) \
            .filter(col("value") != "NaN")


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TemperatureStbBasicAnalytics(configuration, TemperatureStbBasicAnalytics.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
