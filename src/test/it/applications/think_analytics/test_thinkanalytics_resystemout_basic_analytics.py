from applications.basic_analytics.think_analytics.resystemout_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class ThinkAnalyticsResystemoutBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_9_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/think_analytics/basic_analytics/resystemout/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/think_analytics/basic_analytics/resystemout/input",
            expected_result_file="test/it/resources/think_analytics/basic_analytics/resystemout/expected_result.txt"
        )
