from applications.basic_analytics.uservices_analytics.uservices_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class UServicesBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/uservices/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/uservices/input",
            expected_result_file="test/it/resources/basic_analytics/uservices/expected_result.txt"
        )
