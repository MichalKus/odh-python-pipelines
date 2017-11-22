from datetime import datetime

from applications.log_parsing.airflow.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class AirflowLogParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_airflow_dag_execution(self):
        self.assert_parsing(
            {
                "source": "/usr/local/airflow/logs/be_create_obo_assets_transcoding_driven_trigger/lookup_dir/2017-10-10.333",
                "message": """[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Using executor CeleryExecutor
INFO:root:Using connection to: s3.eu-central-1.amazonaws.com
INFO:root:The key lab5a_move_obo_linear_cycle/send_metadata_to_stagis/2017-06-07T00:00:00 now contains 697 bytes
INFO:root:Using connection to: s3.eu-central-1.amazonaws.com
INFO:root:The key lab5a_move_obo_linear_cycle/send_metadata_to_stagis/2017-06-07T00:00:00 now contains 697 bytes"""
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000),
                "dag": "be_create_obo_assets_transcoding_driven_trigger",
                "task": "lookup_dir",
                "level": "INFO",
                "script": "__init__.py:36",
                "message": """Using executor CeleryExecutor
INFO:root:Using connection to: s3.eu-central-1.amazonaws.com
INFO:root:The key lab5a_move_obo_linear_cycle/send_metadata_to_stagis/2017-06-07T00:00:00 now contains 697 bytes
INFO:root:Using connection to: s3.eu-central-1.amazonaws.com
INFO:root:The key lab5a_move_obo_linear_cycle/send_metadata_to_stagis/2017-06-07T00:00:00 now contains 697 bytes"""
            }
        )

    def test_airflow_worker(self):
        self.assert_parsing(
            {
                "source": "/var/logs/airflow.log",
                "message": "[2017-06-09 06:10:36,556] {__init__.py:36} INFO - Using executor CeleryExecutor"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 6, 10, 36, 556000),
                "level": "INFO",
                "script": "__init__.py:36",
                "message": "Using executor CeleryExecutor"
            }

        )
