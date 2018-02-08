from applications.basic_analytics.eos_stb_reports.network_errors_report_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class NetworkErrorsStbBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_network_and_connectivity_errors(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/network_errors_report/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/network_errors_report/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/network_errors_report/expected_result.txt"
        )
