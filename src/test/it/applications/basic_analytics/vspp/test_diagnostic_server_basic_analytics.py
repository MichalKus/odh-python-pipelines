from applications.basic_analytics.vspp.diagnostic_server_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class VsppDiagnosticServerBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_vspp_diagnostic_server_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/vspp/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/vspp/input",
            expected_result_file="test/it/resources/basic_analytics/vspp/expected_result.txt"
        )
