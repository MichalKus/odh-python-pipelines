from applications.basic_analytics.eos_stb_reports.flat.wifi_stats_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbWifiReportsAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/flat/wifi_stats_reports/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/flat/wifi_stats_reports/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/flat/wifi_stats_reports/expected_result.txt"
        )
