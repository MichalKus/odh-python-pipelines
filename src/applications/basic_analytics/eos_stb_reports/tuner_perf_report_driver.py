"""
Aggregate incoming stream of TunerReport messages from a Kafka topic and write to a new Kafka topic
"""

import json

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, FloatType, DoubleType

from common.basic_analytics.aggregations import CompoundAggregation, Sum, Count, Max, Min, Stddev
from common.basic_analytics.aggregations import P01, P05, P10, P25, P50, P75, P90, P95, P99
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TunerPerfReport(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate perf/error metrics related to EOS STB Tuner Report.
    """

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    @staticmethod
    def get_columns(column, num):
        """
        Get multiple columns from an array column based on column name and range
        :param column: Input column for transformation. Output column prefix
        :param num: Output column postfix index
        :return: List of tuples to be used in withColumns call
        """
        ls = []
        for index in range(0, num):
            res = (column + "_" + str(index), col(column)[index])
            ls.append(res)
        return ls

    @staticmethod
    def get_column_names(column_prefix, num=8):
        """
        Get multiple column names with provided prefix and an index until provided num
        :param column_prefix: Prefix for each name
        :param num: Range of numbers from 0 to num to be used as postfix
        :return: List of strings with all column names
        """
        ls = []
        for i in range(0, num):
            ls.append(column_prefix + "_" + str(i))
        return ls

    @staticmethod
    def json_to_array(json_string):
        """
        Converts a string input from JSON format to array consisting of values and array index based on key
        :param json_string: Input JSON string
        :return: Output array of values.
        """
        obj = json.loads(json_string)
        obj = {int(k): float(v) for k, v in obj.items()}
        return [v for _, v in obj.items()]

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "timestamp", "@timestamp")

    def _filter_stream(self, read_stream):
        return read_stream \
            .where("TunerReport_SNR != \"\" AND TunerReport_SNR != \"{}\"") \
            .where("TunerReport_signalLevel != \"\" AND TunerReport_signalLevel != \"{}\"") \
            .where("TunerReport_erroreds != \"\" AND TunerReport_erroreds != \"{}\"") \
            .where("TunerReport_unerroreds != \"\" AND TunerReport_unerroreds != \"{}\"") \
            .where("TunerReport_correcteds != \"\" AND TunerReport_correcteds != \"{}\"")

    def _prepare_input_data_frame(self, read_stream):
        def explode_in_columns(df, columns_with_name):
            """
            Explode a single array column in to multiple columns
            :param df: Input datafram with array column
            :param columns_with_name: List of tupples with column and its name
            :return: Transformed dataframe
            """
            return (reduce(lambda memo_df, col_name:
                           memo_df.withColumn(col_name[0], col_name[1].cast(DoubleType())),
                           columns_with_name, df))

        def expand_df(df, columns):
            """
            Expand a single array column in to multiple columns
            :param df: Input dataframe with array column
            :param columns: list of column names to process
            :return: Dataframe with expanded columns
            """
            return (reduce(
                lambda memo_df, col_name: explode_in_columns(
                    memo_df, TunerPerfReport.get_columns(col_name, 8)),
                columns, df))

        column_list = ["TunerReport_SNR", "TunerReport_signalLevel", "TunerReport_erroreds",
                       "TunerReport_unerroreds", "TunerReport_correcteds"]

        json_to_array_udf = udf(TunerPerfReport.json_to_array, ArrayType(FloatType()))

        input_df = read_stream \
            .withColumn("TunerReport_SNR", json_to_array_udf(col("TunerReport_SNR"))) \
            .withColumn("TunerReport_signalLevel", json_to_array_udf(col("TunerReport_signalLevel"))) \
            .withColumn("TunerReport_erroreds", json_to_array_udf(col("TunerReport_erroreds"))) \
            .withColumn("TunerReport_unerroreds", json_to_array_udf(col("TunerReport_unerroreds"))) \
            .withColumn("TunerReport_correcteds", json_to_array_udf(col("TunerReport_correcteds")))

        return expand_df(input_df, column_list).drop(*column_list)

    def _process_pipeline(self, read_stream):

        pre_result_df = self._prepare_input_data_frame(self._filter_stream(read_stream))

        aggregations_ls = []
        aggregation_fields_without_sum = TunerPerfReport.get_column_names("TunerReport_SNR")
        aggregation_fields_without_sum.extend(TunerPerfReport.get_column_names("TunerReport_signalLevel"))
        aggregation_fields_with_sum = TunerPerfReport.get_column_names("TunerReport_erroreds")
        aggregation_fields_with_sum.extend(TunerPerfReport.get_column_names("TunerReport_unerroreds"))
        aggregation_fields_with_sum.extend(TunerPerfReport.get_column_names("TunerReport_correcteds"))
        aggregations_ls.extend(aggregation_fields_without_sum)
        aggregations_ls.extend(aggregation_fields_with_sum)

        result = []

        for field in aggregations_ls:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field
                      }

            aggregations = [Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                            P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                            P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

            if kwargs["aggregation_field"] in aggregation_fields_with_sum:
                aggregations.append(Sum(**kwargs))

            result.append(pre_result_df.aggregate(CompoundAggregation(aggregations=aggregations, **kwargs)))

        return result

    @staticmethod
    def create_schema():
        """
        Create the input schema according to current processor requirements
        :return: Returns the schema
        """
        return StructType([
            StructField("timestamp", StringType()),
            StructField("hardwareVersion", StringType()),
            StructField("firmwareVersion", StringType()),
            StructField("appVersion", StringType()),
            StructField("asVersion", StringType()),
            StructField("TunerReport_SNR", StringType()),
            StructField("TunerReport_unerroreds", StringType()),
            StructField("TunerReport_erroreds", StringType()),
            StructField("TunerReport_correcteds", StringType()),
            StructField("TunerReport_signalLevel", StringType())
        ])


def create_processor(configuration):
    """
    Creates stream processor object.
    :param config: Configuration object of type Configuration.
    :return: configured TunerPerfReport object.
    """
    return TunerPerfReport(configuration, TunerPerfReport.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
