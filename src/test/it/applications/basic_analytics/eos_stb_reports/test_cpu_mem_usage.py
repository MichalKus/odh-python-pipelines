from applications.basic_analytics.eos_stb_reports.cp_mem_report import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class CpuMemStbBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_cpu_mem_report(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/cpu_mem_usage/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/cpu_mem_usage/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/cpu_mem_usage/expected_result.txt"
        )
