from applications.basic_analytics.eos_stb_reports.flat.memory_usage_report_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbMemoryUsageReportBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_memory_usage_report(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/flat/" +
                               "memory_usage_report/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/flat/memory_usage_report/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/flat/" +
                                 "memory_usage_report/expected_result.txt"
        )
