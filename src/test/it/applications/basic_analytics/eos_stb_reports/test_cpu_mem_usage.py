from applications.basic_analytics.stb_analytics.cpu_analytics import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class CpuMemStbBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_cpu_mem_report(self):
        self._test_pipeline(
            configuration_path="/home/john/odh-python-pipelines/src/test/it/resources/basic_analytics/eos_stb_reports/cpu_mem_usage/configuration.yml",
            processor_creator=create_processor,
            input_dir="/home/john/odh-python-pipelines/src/test/it/resources/basic_analytics/eos_stb_reports/cpu_mem_usage/input",
            expected_result_file="/home/john/odh-python-pipelines/src/test/it/resources/basic_analytics/eos_stb_reports/cpu_mem_usage/expected_result.txt"
        )
