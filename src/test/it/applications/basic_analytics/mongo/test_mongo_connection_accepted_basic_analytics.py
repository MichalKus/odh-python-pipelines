from applications.basic_analytics.mongo.mongo_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class MongoBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_10_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/mongo/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/mongo/input",
            expected_result_file="test/it/resources/basic_analytics/mongo/expected_result.txt"
        )
