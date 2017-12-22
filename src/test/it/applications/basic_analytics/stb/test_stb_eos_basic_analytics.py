from applications.basic_analytics.stb_eos.driver import StbEosProcessor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbEosAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb/eos/configuration.yml",
            processor_creator=StbEosProcessor.create_processor,
            input_dir="test/it/resources/basic_analytics/stb/eos/input",
            expected_result_file="test/it/resources/basic_analytics/stb/eos/expected_result.txt"
        )
