from applications.basic_analytics.stb_general.cpe_ethernet_wifi_reports_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbEthernetWifiReportsAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb_general/ethernet_wifi_reports/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_general/ethernet_wifi_reports/input",
            expected_result_file="test/it/resources/basic_analytics/stb_general/ethernet_wifi_reports/expected_result.txt"
        )
