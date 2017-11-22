from applications.basic_analytics.think_analytics.httpaccess_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class ThinkAnalyticsHttpAccessBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_300_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/think_analytics/httpaccess/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/think_analytics/httpaccess/input",
            expected_result_file="test/it/resources/basic_analytics/think_analytics/httpaccess/expected_result.txt"
        )
