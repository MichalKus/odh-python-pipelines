import unittest
import json
import uuid

from common.test_pipeline import TestPipeline
from util.utils import Utils


class BaseSparkProcessorTestCase(unittest.TestCase):
    def _test_pipeline(self, configuration_path, processor_creator, input_dir, expected_result_file,
                       table_name="test_result", timeout=10, print_result=False):
        """
        Method for checking equals between driver result and manually generated result file
        :param configuration_path: path to config file
        :param processor_creator: processor for event_creator
        :param input_dir: path to input messages
        :param expected_result_file: path to expected result
        :param table_name: name for table with result in memory
        :param timeout: timout for waiting driver end its work before matching result
        :param print_result: flag for only printing results to console for debugging
        :return:
        """
        table_uuid_postfix = "_" + str(uuid.uuid1()).replace("-", "_")
        configuration = Utils.load_config(configuration_path)
        pipeline = TestPipeline(
            configuration,
            processor_creator(configuration),
            input_dir,
            table_name + table_uuid_postfix
        )
        pipeline.start(timeout)
        result_tables_list = [[json.loads(row.value) for row in
                               pipeline.spark.sql("select value from " + query.name).collect()]
                              for query in pipeline.spark.streams.active]
        result = [table for results in result_tables_list for table in results]
        pipeline.terminate_active_streams()
        if print_result:
            print(result)
        else:
            expected_result = self.__read_expected_result(expected_result_file)
            self.maxDiff = None
            self.assertItemsEqual(expected_result, result)

    @staticmethod
    def __read_expected_result(expected_result_file):
        with open(expected_result_file) as json_file:
            lines = filter(
                lambda json_line: json_line.strip() != "",
                json_file.readlines()
            )
        return [json.loads(line) for line in lines]
