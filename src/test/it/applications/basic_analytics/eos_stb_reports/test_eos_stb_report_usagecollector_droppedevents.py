from applications.basic_analytics.eos_stb.general_driver import GeneralEosStbProcessor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class EosStbAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb/general/configuration.yml",
            processor_creator=GeneralEosStbProcessor.create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb/general/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb/general/expected_result.txt"
        )
