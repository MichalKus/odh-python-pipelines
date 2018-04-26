from applications.basic_analytics.stb_general.uservice_service_requests_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class UserviceServiceRequestTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb_general/uservice_service_request/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_general/uservice_service_request/input",
            expected_result_file="test/it/resources/basic_analytics/stb_general/uservice_service_request/expected_result.txt"
        )
