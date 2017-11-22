from applications.basic_analytics.traxis_cassandra.error_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class TraxisCassandraGeneralBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_300_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/traxis_cassandra/error/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/traxis_cassandra/error/input",
            expected_result_file="test/it/resources/basic_analytics/traxis_cassandra/error/expected_result.txt"
        )
