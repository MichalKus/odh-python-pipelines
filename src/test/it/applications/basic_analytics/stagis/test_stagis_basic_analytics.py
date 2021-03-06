from applications.basic_analytics.stagis.stagis_basic_analytics import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StagisBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_stagis_basic_analytics(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stagis/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stagis/input",
            expected_result_file="test/it/resources/basic_analytics/stagis/expected_result.txt"
        )
