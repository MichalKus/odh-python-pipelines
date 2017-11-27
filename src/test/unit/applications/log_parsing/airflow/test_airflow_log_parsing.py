from datetime import datetime

from applications.log_parsing.airflow.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class AirflowLogParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_airflow_dag_execution_without_subtask(self):
        self.assert_parsing(
            {
                "source": "/usr/local/airflow/logs/bbc_lookup_programmes_workflow/lookup_and_update_programmes/2017-10-10.333",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000),
                "dag": "bbc_lookup_programmes_workflow",
                "task": "lookup_and_update_programmes",
                "level": "INFO",
                "message": "Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']",
                "script": "base_task_runner.py:113",
            }
        )

    def test_airflow_dag_execution_with_subtask(self):
        self.assert_parsing(
            {
                "source": "/usr/local/airflow/logs/be_create_obo_assets_transcoding_driven_trigger/lookup_dir/2017-10-10.333",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000),
                "dag": "be_create_obo_assets_transcoding_driven_trigger",
                "task": "lookup_dir",
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk",

                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 3, 05, 555000),
                "subtask_script": "base_hook.py",
                "subtask_level": "INFO",
                "subtask_message": "Using connection to: media-syndication.api.bbci.co.uk"
            }
        )

    def test_airflow_dag_execution_with_subtask_and_crid(self):
        self.assert_parsing(
            {
                "source": "/usr/local/airflow/logs/be_create_obo_assets_transcoding_driven_trigger/lookup_dir/2017-10-10.333",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000),
                "dag": "be_create_obo_assets_transcoding_driven_trigger",
                "task": "lookup_dir",
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 5, 8, 555000),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "crid": "crid://og.libertyglobal.com/MTV/PAID0000000001432979",
            }
        )

    def test_airflow_dag_execution_with_subtask_and_airflow_id(self):
        self.assert_parsing(
            {
                "source": "/usr/local/airflow/logs/create_obo_assets_transcoded_workflow/ingest_to_fabrix/2017-10-10.333",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000),
                "dag": "create_obo_assets_transcoded_workflow",
                "task": "ingest_to_fabrix",
                "level": "INFO",
                "message": "Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "script": "base_task_runner.py:113",
                "subtask_timestamp": datetime(2017, 10, 27, 9, 55, 25, 555000),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "airflow_id": "92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
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

    def test_airflow_without_subtask(self):
        self.assert_parsing(
            {
                "source": "/var/logs/airflow.log",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000),
                "level": "INFO",
                "message": "Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']",
                "script": "base_task_runner.py:113",
            }
        )

    def test_airflow_with_subtask(self):
        self.assert_parsing(
            {
                "source": "/var/logs/airflow.log",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000),
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk",
                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 3, 05, 555000),
                "subtask_script": "base_hook.py",
                "subtask_level": "INFO",
                "subtask_message": "Using connection to: media-syndication.api.bbci.co.uk"
            }
        )

    def test_airflow_with_subtask_and_crid(self):
        self.assert_parsing(
            {
                "source": "/var/logs/airflow.log",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000),
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 5, 8, 555000),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "crid": "crid://og.libertyglobal.com/MTV/PAID0000000001432979",
            }
        )

    def test_airflow_with_subtask_and_airflow_id(self):
        self.assert_parsing(
            {
                "source": "/var/logs/airflow.log",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000),
                "level": "INFO",
                "message": "Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "script": "base_task_runner.py:113",
                "subtask_timestamp": datetime(2017, 10, 27, 9, 55, 25, 555000),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "airflow_id": "92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
            }
        )
