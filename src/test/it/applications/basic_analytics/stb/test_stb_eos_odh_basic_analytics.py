from applications.basic_analytics.stb_eos.odh_driver import StbEosOdhProcessor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbEosAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb/eos_odh/configuration.yml",
            processor_creator=StbEosOdhProcessor.create_processor,
            input_dir="test/it/resources/basic_analytics/stb/eos_odh/input",
            expected_result_file="test/it/resources/basic_analytics/stb/eos_odh/expected_result.txt",
            print_result=True
        )
