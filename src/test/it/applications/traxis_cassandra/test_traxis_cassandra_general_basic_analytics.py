from applications.basic_analytics.traxis_cassandra.general_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class TraxisCassandraGeneralBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_300_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/traxis_cassandra/basic_analytics/general/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/traxis_cassandra/basic_analytics/general/input",
            expected_result_file="test/it/resources/traxis_cassandra/basic_analytics/general/expected_result.txt"
        )
