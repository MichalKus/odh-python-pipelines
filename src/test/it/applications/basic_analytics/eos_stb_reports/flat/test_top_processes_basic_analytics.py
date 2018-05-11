from applications.basic_analytics.eos_stb_reports.flat.top_processes_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbTopProcessesBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_event(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/flat/top_processes/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/flat/top_processes/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/flat/top_processes"
                                 "/expected_result.txt"
        )
