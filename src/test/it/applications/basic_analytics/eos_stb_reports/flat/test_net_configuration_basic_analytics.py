from applications.basic_analytics.eos_stb_reports.flat.net_configuration_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbNetConfigurationReportsAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/flat/net_configuration_reports/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/flat/net_configuration_reports/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/flat/net_configuration_reports/expected_result.txt"
        )
