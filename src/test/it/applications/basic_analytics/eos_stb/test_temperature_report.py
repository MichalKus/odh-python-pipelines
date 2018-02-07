from applications.basic_analytics.stb_analytics.temperature_report import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class TemperatureStbBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_temperature_report(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb_analytics/temperature_report/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_analytics/temperature_report/input",
            expected_result_file="test/it/resources/basic_analytics/stb_analytics/temperature_report/expected_result.txt"
        )
