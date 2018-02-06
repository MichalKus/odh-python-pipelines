import json

from pyspark.sql.functions import col, udf, from_unixtime
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, ArrayType, FloatType, DoubleType, \
    IntegerType

from common.basic_analytics.aggregations import CompoundAggregation, Sum, Count, Max, Min, Stddev, P01, P05, P10, P25, P50, P75, P90, P95, P99
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TunerPerfReport(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate perf/error metrics related to EOS STB Tuner Report.
    """

    __dimensions = ["hardwareVersion", "firmwareVersion", "appVersion", "asVersion"]

    @staticmethod
    def get_column(column, dest_col, index):
        return [(dest_col, col(column)[index])]

    @staticmethod
    def get_columns(column, num):
        ls = []
        for i in range(0, num):
            res = TunerPerfReport.get_column(column, column + "_" + str(i), i)
            ls.extend(res)
        return ls

    @staticmethod
    def get_column_names(column_prefix, num = 8):
        ls = []
        for i in range(0, num):
            ls.append(column_prefix + "_" + str(i))
        return ls

    @staticmethod
    def json_to_array(json_string):
        obj = json.loads(json_string)
        obj = {int(k): float(v) for k, v in obj.items()}
        return [v for _, v in obj.items()]

    def _prepare_timefield(self, data_stream):
        return data_stream.withColumn("@timestamp", from_unixtime(col("timestamp") / 1000).cast(TimestampType()))


    def _process_pipeline(self, read_stream):
        def apply_udf(udf_func, column_list):
            ls = []
            print ["column_list is"] + column_list
            return (reduce(
                lambda memo_list, col_name: memo_list.extend([udf_func(col_name).alias(col_name)]),
                column_list,
                ls)
            )

        def explode_in_columns(df, columns_with_name):
            return (reduce(
                lambda memo_df, col_name: memo_df.withColumn(col_name[0],
                                                             col_name[1] #.cast(IntegerType())
                                                             ),
                columns_with_name,
                df))

        def expand_df(df, columns):
            return (reduce(
                lambda memo_df, col_name: explode_in_columns(memo_df, TunerPerfReport.get_columns(col_name, 8)),
                columns,
                df))

        column_list = ["TunerReport_SNR", "TunerReport_signalLevel", "TunerReport_erroreds"
            , "TunerReport_unerroreds", "TunerReport_correcteds"]

        # expanded_dataframe = \
        #     explode_in_columns(
        #         explode_in_columns(
        #             explode_in_columns(
        #                 explode_in_columns(
        #                     explode_in_columns(
        #                         read_stream,
        #                         get_columns("TunerReport_SNR", 8)
        #                     ), get_columns("TunerReport_signalLevel", 8)
        #                 ), get_columns("TunerReport_erroreds", 8)
        #             ), get_columns("TunerReport_unerroreds", 8)
        #         ), get_columns("TunerReport_correcteds", 8)
        #     ).drop(*column_list)
        json_to_array_udf = udf(TunerPerfReport.json_to_array, ArrayType(FloatType()))
        # read_stream.printSchema()

        # read_stream \
        #     .writeStream \
        #     .format("console") \
        #     .trigger(processingTime='2 seconds') \
        #     .outputMode("append") \
        #     .start()

        # .withColumnRenamed("timestamp", "@timestamp") \
        input_df = read_stream \
            .withColumn("TunerReport_SNR", json_to_array_udf(col("TunerReport_SNR"))) \
            .withColumn("TunerReport_signalLevel", json_to_array_udf(col("TunerReport_signalLevel"))) \
            .withColumn("TunerReport_erroreds", json_to_array_udf(col("TunerReport_erroreds"))) \
            .withColumn("TunerReport_unerroreds", json_to_array_udf(col("TunerReport_unerroreds"))) \
            .withColumn("TunerReport_correcteds", json_to_array_udf(col("TunerReport_correcteds")))
        # .select("@timestamp", *apply_udf(json_to_array_udf, column_list))

        expanded_dataframe = expand_df(input_df, column_list)

        pre_result_df = expanded_dataframe.drop(*column_list)
        # pre_result_df.printSchema()


        # results_df = pre_result_df \
        #     .aggregate(Count(aggregation_field="TunerReport_SNR_0", aggregation_name=self._component_name))




        # results_df.results("2 seconds", "@timestamp")[0] \
        # pre_result_df \
        #     .writeStream\
        #     .format("console") \
        #     .trigger(processingTime='2 seconds') \
        #     .outputMode("update") \
        #     .start()



        # return [results_df]

        aggregation_fields_without_sum = TunerPerfReport.get_column_names("TunerReport_SNR")
        aggregation_fields_without_sum.extend(TunerPerfReport.get_column_names("TunerReport_signalLevel"))

        aggregation_fields_with_sum = TunerPerfReport.get_column_names("TunerReport_erroreds")
        aggregation_fields_with_sum.extend(TunerPerfReport.get_column_names("TunerReport_unerroreds"))
        aggregation_fields_with_sum.extend(TunerPerfReport.get_column_names("TunerReport_correcteds"))

        result = []

        for field in aggregation_fields_without_sum:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field
                      }

            aggregations = [Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                            P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                            P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

            column_stream = pre_result_df.aggregate(CompoundAggregation(aggregations=aggregations, **kwargs))

            # column_stream \
            # .results("10 seconds", "@timestamp")[0] \
            # .writeStream\
            #     .format("console").outputMode("update") \
            #         .option("truncate", False).option("numRows", 100) \
            #         .start()

            result.append(column_stream)

        for field in aggregation_fields_with_sum:
            kwargs = {'group_fields': self.__dimensions,
                      'aggregation_name': self._component_name,
                      'aggregation_field': field}

            aggregations = [Sum(**kwargs), Count(**kwargs), Max(**kwargs), Min(**kwargs), Stddev(**kwargs),
                            P01(**kwargs), P05(**kwargs), P10(**kwargs), P25(**kwargs), P50(**kwargs),
                            P75(**kwargs), P90(**kwargs), P95(**kwargs), P99(**kwargs)]

            column_stream = pre_result_df.aggregate(CompoundAggregation(aggregations=aggregations, **kwargs))

            result.append(column_stream)

        return result


    @staticmethod
    def create_schema():
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
    """Method to create the instance of the processor"""
    return TunerPerfReport(configuration, TunerPerfReport.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
