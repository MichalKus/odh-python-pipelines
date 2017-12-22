from applications.basic_analytics.airflow.worker_dag_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class AirflowWorkerSuccessFailureRateBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_success_failure_rate(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/airflow/success_failure_rate/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/airflow/success_failure_rate/input",
            expected_result_file="test/it/resources/basic_analytics/airflow/success_failure_rate/expected_result.txt"
        )
