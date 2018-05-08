from applications.basic_analytics.airflow.manager_scheduler_latest_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class AirflowManagerSchedulerLatestBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/airflow/manager_scheduler_latest/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/airflow/manager_scheduler_latest/input",
            expected_result_file="test/it/resources/basic_analytics/airflow/manager_scheduler_latest/expected_result.txt"
        )
