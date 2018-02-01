from applications.basic_analytics.stb_analytics.network_errors import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class NetworkErrorsStbBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_network_and_connectivity_errors(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb_analytics/network_errors/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_analytics/network_errors/input",
            expected_result_file="test/it/resources/basic_analytics/stb_analytics/network_errors/expected_result.txt"
        )
