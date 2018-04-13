from applications.basic_analytics.catalina.catalina_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class CatalinaBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_10_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/catalina/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/catalina/input",
            expected_result_file="test/it/resources/basic_analytics/catalina/expected_result.txt"
        )
