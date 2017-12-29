from applications.basic_analytics.eos_stb.odh_driver import EosStbOdhProcessor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class EosStbAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb/odh/configuration.yml",
            processor_creator=EosStbOdhProcessor.create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb/odh/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb/odh/expected_result.txt"
        )
