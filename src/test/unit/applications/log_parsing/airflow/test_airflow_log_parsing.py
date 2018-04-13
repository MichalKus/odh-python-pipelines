from datetime import datetime

from dateutil.tz import tzoffset

from applications.log_parsing.airflow.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration
from common.log_parsing.timezone_metadata import timezones

class AirflowLogParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_airflow_dag_execution_without_subtask(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/usr/local/airflow/logs/bbc_lookup_programmes_workflow/lookup_and_update_programmes/2017-10-10.333",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "dag": "bbc_lookup_programmes_workflow",
                "task": "lookup_and_update_programmes",
                "level": "INFO",
                "message": "Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']",
                "script": "base_task_runner.py:113"
            }
        )

    def test_airflow_dag_execution_with_subtask(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/usr/local/airflow/logs/be_create_obo_assets_transcoding_driven_trigger/lookup_dir/2017-10-10.333",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "dag": "be_create_obo_assets_transcoding_driven_trigger",
                "task": "lookup_dir",
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk",

                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 3, 05, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "subtask_script": "base_hook.py",
                "subtask_level": "INFO",
                "subtask_message": "Using connection to: media-syndication.api.bbci.co.uk"
            }
        )

    def test_airflow_dag_execution_with_subtask_and_crid(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/usr/local/airflow/logs/be_create_obo_assets_transcoding_driven_trigger/lookup_dir/2017-10-10.333",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "dag": "be_create_obo_assets_transcoding_driven_trigger",
                "task": "lookup_dir",
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 5, 8, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "crid": "crid://og.libertyglobal.com/MTV/PAID0000000001432979"
            }
        )

    def test_airflow_dag_execution_with_subtask_and_airflow_id(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/usr/local/airflow/logs/create_obo_assets_transcoded_workflow/ingest_to_fabrix/2017-10-10.333",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "dag": "create_obo_assets_transcoded_workflow",
                "task": "ingest_to_fabrix",
                "level": "INFO",
                "message": "Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "script": "base_task_runner.py:113",
                "subtask_timestamp": datetime(2017, 10, 27, 9, 55, 25, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "airflow_id": "92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018"
            }
        )

    def test_airflow_worker(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/var/logs/airflow.log",
                "message": "[2017-06-09 06:10:36,556] {__init__.py:36} INFO - Using executor CeleryExecutor"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 6, 10, 36, 556000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "script": "__init__.py:36",
                "message": "Using executor CeleryExecutor"
            }

        )

    def test_airflow_without_subtask(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/var/logs/airflow.log",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "Running: ['bash', '-c', u'airflow run bbc_lookup_programmes_workflow lookup_and_update_programmes 2017-11-27T06:55:09 --job_id 546290 --queue bbc --raw -sd DAGS_FOLDER/bbc_lookup_programmes_workflow.py']",
                "script": "base_task_runner.py:113"
            }
        )

    def test_airflow_with_subtask(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/var/logs/airflow.log",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:03:05,555] {base_hook.py:67} INFO - Using connection to: media-syndication.api.bbci.co.uk",
                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 3, 05, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "subtask_script": "base_hook.py",
                "subtask_level": "INFO",
                "subtask_message": "Using connection to: media-syndication.api.bbci.co.uk"
            }
        )

    def test_airflow_with_subtask_and_crid(self):
        self.assert_parsing(
            {
                "topic":"airflow_worker",
                "source": "/var/logs/airflow.log",
                "message": "[2017-06-09 09:03:03,399] {__init__.py:36} INFO - Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979"
            },
            {
                "@timestamp": datetime(2017, 06, 9, 9, 3, 03, 399000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "Subtask: [2017-06-09 09:05:08,555] {create_obo_assets_transcoded_workflow.py:224} INFO - Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "script": "__init__.py:36",
                "subtask_timestamp": datetime(2017, 06, 9, 9, 5, 8, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Fabrix input: /obo_manage/Countries/UK/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2FMTV~~2FPAID0000000001432979",
                "crid": "crid://og.libertyglobal.com/MTV/PAID0000000001432979"
            }
        )

    def test_airflow_with_subtask_and_airflow_id(self):
        self.assert_parsing(
            {
                "topic": "airflow_worker",
                "source": "/var/logs/airflow.log",
                "message": "[2017-10-27 09:55:24,555] {base_task_runner.py:113} INFO - Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "Subtask: [2017-10-27 09:55:25,555] {create_obo_assets_transcoded_workflow.py:217} INFO - Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "script": "base_task_runner.py:113",
                "subtask_timestamp": datetime(2017, 10, 27, 9, 55, 25, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "subtask_script": "create_obo_assets_transcoded_workflow.py",
                "subtask_level": "INFO",
                "subtask_message": "Submitting asset: 92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018",
                "airflow_id": "92bf0465527a60db913f3490e5ce905b_3371E5144AD4597D56709497CB31A018"
            }
        )

    def test_airflow_manager_with_dag(self):
        self.assert_parsing(
            {
                "topic": "airflowmanager_scheduler_latest",
                "source": "any.log",
                "message": "[2017-10-27 09:55:24,555] {jobs.py:1537} DagFileProcessor72328 INFO - DAG(s) ['be_create_obo_thumbnails_workflow'] retrieved from /usr/local/airflow/dags/be_create_obo_thumbnails_workflow.py"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "message_level": "INFO",
                "message": "{jobs.py:1537} DagFileProcessor72328 INFO - DAG(s) ['be_create_obo_thumbnails_workflow'] retrieved from /usr/local/airflow/dags/be_create_obo_thumbnails_workflow.py",
                "script_name": "jobs.py",
                "dag": "be_create_obo_thumbnails_workflow"
            }
        )

    def test_airflow_manager_without_dag(self):
        self.assert_parsing(
            {
                "topic": "airflowmanager_scheduler_latest",
                "source": "any.log",
                "message": "[2017-10-27 09:55:24,555] {models.py:4204} DagFileProcessor72223 INFO - Updating state for <DagRun be_create_obo_assets_transcoding_driven_workflow @ 2018-03-06 15:24:17.806572: be-crid~~3A~~2F~~2Ftelenet.be~~2F8ebcb1e0-8295-40b4-b5ee-fa6c0dd329a6-2018-03-06T15:20:50.800499, externally triggered: True> considering 20 task(s)"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "message_level": "INFO",
                "message": "{models.py:4204} DagFileProcessor72223 INFO - Updating state for <DagRun be_create_obo_assets_transcoding_driven_workflow @ 2018-03-06 15:24:17.806572: be-crid~~3A~~2F~~2Ftelenet.be~~2F8ebcb1e0-8295-40b4-b5ee-fa6c0dd329a6-2018-03-06T15:20:50.800499, externally triggered: True> considering 20 task(s)",
                "script_name": "models.py"
            }
        )
    def test_manager_scheduler_airflow(self):
        self.assert_parsing(
            {
                "topic": "airflowmanager_scheduler_airflow",
                "message": "[2017-10-27 09:55:24,555] {jobs.py:1195} INFO - Executor reports be_create_obo_assets_transcoding_driven_workflow.register_on_license_server execution_date=2018-04-13 09:20:53.573308 as success"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "message_level": "INFO",
                "message": "{jobs.py:1195} INFO - Executor reports be_create_obo_assets_transcoding_driven_workflow.register_on_license_server execution_date=2018-04-13 09:20:53.573308 as success",
                "script_name": "jobs.py"
            }
        )

    def test_airflow_manager_webui_without_script(self):
        self.assert_parsing(
            {
                "topic": "airflowmanager_webui",
                "source": "any.log",
                "message": "[2018-04-12 10:12:16 +0000] [16262] [INFO] Booting worker with pid: 16262"
            },
            {
                "@timestamp": datetime(2018, 4, 12, 10, 12, 16, 0).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "message_level": "INFO",
                "message": "[16262] [INFO] Booting worker with pid: 16262"
            }
        )

    def test_airflow_manager_webui_with_script(self):
        self.assert_parsing(
            {
                "topic": "airflowmanager_webui",
                "source": "any.log",
                "message": "[2017-10-27 09:55:24,555] [16262] {models.py:168} INFO - Filling up the DagBag from /usr/local/airflow/dags"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 9, 55, 24, 555000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "message_level": "INFO",
                "message": "[16262] {models.py:168} INFO - Filling up the DagBag from /usr/local/airflow/dags",
                "script_name": "models.py"
            }
        )
