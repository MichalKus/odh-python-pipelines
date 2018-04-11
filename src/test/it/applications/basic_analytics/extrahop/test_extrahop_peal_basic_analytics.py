from applications.basic_analytics.extrahop.peal_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class ExtrahopPealBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_peal_data_analytics(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/extrahop/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/extrahop/input",
            expected_result_file="test/it/resources/basic_analytics/extrahop/expected_result.txt"
        )
