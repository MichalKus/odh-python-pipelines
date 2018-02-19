import uuid
import json

from applications.adv_analytics.cloudmap_vm_correlation.driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase
from common.test_pipeline import TestPipeline
from util.utils import Utils

class VropsCloudmapVMCorrelationTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/adv_analytics/vrops/cloudmap_vm_correlation/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/adv_analytics/vrops/cloudmap_vm_correlation/input",
            expected_result_file="test/it/resources/adv_analytics/vrops/cloudmap_vm_correlation/expected_result.txt"
        )

    def _test_pipeline(self, configuration_path, processor_creator, input_dir, expected_result_file,
                       timeout=10, print_result=False):
        """
        Method for checking equals between driver result and manually generated result file
        :param configuration_path: path to config file
        :param processor_creator: processor for event_creator
        :param input_dir: path to input messages
        :param expected_result_file: path to expected result
        :param timeout: timout for waiting driver end its work before matching result
        :param print_result: flag for only printing results to console for debugging
        :return:
        """
        table_uuid_postfix = "_" + str(uuid.uuid1()).replace("-", "_")
        configuration = Utils.load_config(configuration_path)
        pipeline = CustomTestPipeline(
            configuration,
            processor_creator(configuration),
            input_dir,
            "test_result" + table_uuid_postfix
        )
        pipeline.start(timeout)
        result_tables_list = [[json.loads(row.value) for row in
                               pipeline.spark.sql("select value from " + query.name).collect()]
                              for query in pipeline.spark.streams.active]
        result = [table for results in result_tables_list for table in results]
        pipeline.terminate_active_streams()
        if print_result:
            for row in result:
                print(row)
        else:
            expected_result = self._BaseSparkProcessorTestCase__read_expected_result(expected_result_file)
            self.maxDiff = None
            self.assertItemsEqual(expected_result, result)

class CustomTestPipeline(TestPipeline):
    """
    Custom Test Pipeline to allow batch processing from HDFS
    """
    def _create_custom_read_stream(self, spark):
        return [spark, spark.readStream.text(self._TestPipeline__input_dir).where("value != ''")]
