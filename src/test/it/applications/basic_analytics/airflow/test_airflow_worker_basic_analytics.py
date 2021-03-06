from applications.basic_analytics.airflow.worker_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class AirflowWorkerBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_300_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/airflow/worker/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/airflow/worker/input",
            expected_result_file="test/it/resources/basic_analytics/airflow/worker/expected_result.txt"
        )
