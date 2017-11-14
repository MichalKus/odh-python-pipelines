from applications.basic_analytics.traxis_backend.error_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class TraxisBackendErrorBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_300_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/traxis_backend/basic_analytics/error/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/traxis_backend/basic_analytics/error/input",
            expected_result_file="test/it/resources/traxis_backend/basic_analytics/error/expected_result.txt"
        )
