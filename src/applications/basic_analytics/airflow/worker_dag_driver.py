"""
The module for the driver to calculate metrics related to DAGs in the Airflow Worker component.
See:
  ODH-1439: Success/failure extension of Airflow-Worker basics analytics job
  ODH-1442: Airflow. Running DAGs per hosts
"""

from pyspark.sql.functions import col, lit, when, regexp_extract
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count, DistinctCount, Sum
from common.spark_utils.custom_functions import custom_translate_like
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class AirflowWorkerDag(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to DAGs in the Airflow Worker component.
    """

    def _process_pipeline(self, read_stream):
        """
        Main pipeline for concatenation all aggregation queries in one list
        :param read_stream: input stream with events from dag kafka topic
        :return: list of all aggregated metrics
        """
        return self.__process_common_events(read_stream) + self.__process_tva_events(read_stream) + \
               self.__process_hi_res_events(read_stream) + self.__process_hi_res_on_mpx_events(read_stream)

    def __process_common_events(self, read_stream):
        """
        Aggregation for events to calculate common metrics
        :param read_stream: input stream with events from dag kafka topic
        :return: list of aggregated metrics
        """
        dag_count = read_stream \
            .select(col("hostname"), col("@timestamp"), col("dag")) \
            .aggregate(DistinctCount(group_fields=["hostname"], aggregation_field="dag",
                                     aggregation_name=self._component_name))

        success_and_failures_counts = read_stream \
            .select(col("@timestamp"), col("task"), col("dag"), col("message")) \
            .where(col("message").like("Task exited with return code%")) \
            .withColumn("status",
                        when(col("message").like("Task exited with return code 0%"), lit("success"))
                        .otherwise(lit("failure"))) \
            .aggregate(Count(group_fields=["dag", "task", "status"], aggregation_name=self._component_name))

        return [dag_count, success_and_failures_counts]

    def __process_tva_events(self, read_stream):
        """
        Aggregation for events with information about TVA
        :param read_stream: input stream with events from dag kafka topic
        :return: list of aggregated metrics
        """
        fetch_and_process_tva_file_events = read_stream \
            .where("task == 'fetch_and_process_tva_file'")

        tva_downloads_count = fetch_and_process_tva_file_events \
            .where("subtask_message like 'Downloading%'") \
            .aggregate(Count(group_fields=["dag", "task"], aggregation_name=self._component_name + ".tva_downloads"))

        tva_images_to_process_sum = fetch_and_process_tva_file_events \
            .where("subtask_message like 'Images batch to process%images to process%'") \
            .withColumn("images_to_process",
                        regexp_extract("subtask_message", r"^Images batch to process:\s+(\d+).*", 1)) \
            .aggregate(Sum(group_fields=["dag", "task"], aggregation_field="images_to_process",
                           aggregation_name=self._component_name + ".tva_images_to_process"))

        tva_images_processed_events = fetch_and_process_tva_file_events \
            .where("subtask_message like 'Images processed: creating:%updating%'")

        tva_images_processed_creating_sum = tva_images_processed_events \
            .withColumn("images_creating",
                        regexp_extract("subtask_message", r"^Images processed: creating:\s+(\d+).*", 1)) \
            .aggregate(Sum(group_fields=["dag", "task"], aggregation_field="images_creating",
                           aggregation_name=self._component_name + ".tva_images_processed_creating"))

        tva_images_processed_updating_sum = tva_images_processed_events \
            .withColumn("images_updating",
                        regexp_extract("subtask_message", r".*updating:\s+(\d+).*", 1)) \
            .aggregate(Sum(group_fields=["dag", "task"], aggregation_field="images_updating",
                           aggregation_name=self._component_name + ".tva_images_processed_updating"))

        return [tva_downloads_count, tva_images_to_process_sum,
                tva_images_processed_creating_sum, tva_images_processed_updating_sum]

    def __process_hi_res_events(self, read_stream):
        """
        Aggregation for events with information about loading high_resolution images
        :param read_stream: input stream with events from dag kafka topic
        :return: list of aggregated metrics
        """
        perform_high_res_images_events = read_stream \
            .where("task == 'perform_high_resolution_images_qc'")

        perform_high_res_images_processed_success_sum = perform_high_res_images_events \
            .where("subtask_message like 'Images processed:%'") \
            .withColumn("images_success",
                        regexp_extract("subtask_message",
                                       r"^Images processed: qc_success: (\d+), qc_retry: (\d+), qc_error: (\d+).*", 1)) \
            .aggregate(Sum(group_fields=["dag", "task"], aggregation_field="images_success",
                           aggregation_name=self._component_name + ".hi_res_images_processed_success"))

        perform_high_res_images_processed_retry_sum = perform_high_res_images_events \
            .where("subtask_message like 'Images processed:%'") \
            .withColumn("images_retry",
                        regexp_extract("subtask_message",
                                       r"^Images processed: qc_success: (\d+), qc_retry: (\d+), qc_error: (\d+).*", 2)) \
            .aggregate(Sum(group_fields=["dag", "task"], aggregation_field="images_retry",
                           aggregation_name=self._component_name + ".hi_res_images_processed_retry"))

        perform_high_res_images_processed_error_sum = perform_high_res_images_events \
            .where("subtask_message like 'Images processed:%'") \
            .withColumn("images_error",
                        regexp_extract("subtask_message",
                                       r"^Images processed: qc_success: (\d+), qc_retry: (\d+), qc_error: (\d+).*", 3)) \
            .aggregate(Sum(group_fields=["dag", "task"], aggregation_field="images_error",
                           aggregation_name=self._component_name + ".hi_res_images_processed_error"))

        __mapping_image_type = [
            (["image_type='HighResPortrait'", "status='qc_success'"], "hi_res_images_portrait"),
            (["image_type='HighResLandscape'", "status='qc_success'"], "hi_res_images_landscape")
        ]

        perform_high_res_images_type_count = perform_high_res_images_events \
            .withColumn("image_type", custom_translate_like(source_field=col("subtask_message"),
                                                            mappings_pair=__mapping_image_type,
                                                            default_value="unclassified")) \
            .where("image_type != 'unclassified'") \
            .aggregate(Count(group_fields=["dag", "task", "image_type"],
                             aggregation_name=self._component_name))

        return [perform_high_res_images_processed_success_sum, perform_high_res_images_processed_retry_sum,
                perform_high_res_images_processed_error_sum, perform_high_res_images_type_count
                ]

    def __process_hi_res_on_mpx_events(self, read_stream):
        """
        Aggregation for events with information about high_resolution and loading to mpx
        :param read_stream: input stream with events from dag kafka topic
        :return: list of aggregated metrics
        """
        upload_high_res_images_created_on_mpx_count = read_stream \
            .where("task == 'upload_high_resolution_images_to_mpx'") \
            .where("subtask_message like '%Image was created on MPX:%'") \
            .aggregate(Count(group_fields=["dag", "task"],
                             aggregation_name=self._component_name + ".hi_res_images_created_on_mpx"))

        return [upload_high_res_images_created_on_mpx_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("message", StringType()),
            StructField("hostname", StringType()),
            StructField("task", StringType()),
            StructField("dag", StringType()),
            StructField("subtask_message", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return AirflowWorkerDag(configuration, AirflowWorkerDag.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
