from applications.basic_analytics.think_analytics.reingest_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class ThinkAnalyticsReingestBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_10_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/think_analytics/reingest/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/think_analytics/reingest/input",
            expected_result_file="test/it/resources/basic_analytics/think_analytics/reingest/expected_result.txt"
        )
