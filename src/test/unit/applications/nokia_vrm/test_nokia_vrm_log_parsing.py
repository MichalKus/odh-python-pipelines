from datetime import datetime

from applications.log_parsing.nokia_vrm.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class NokiaVrmMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_scheduler_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-scheduler-BS/logs/scheduler_bs_audit.log",
                "message": "29-Oct-2017 21:49:12.672 | INFO  | 0409217a-7120-4afd-8939-49c142749ab8 | INTERNAL |  | [[GET] /scheduler/web/Record/addByProgram] | [{schema=1.0, eventId=crid:~~2F~~2Ftelenet.be~~2Fd3cb0337-2cad-45a5-9252-5ad7711354e8,imi:0010000000075BC2, userId=03e30b70-106c-416b-8243-40fe1e7f981b_be}] | Entity not found (info) | Not found Event entity with programId field equals to crid:~~2F~~2Ftelenet.be~~2Fd3cb0337-2cad-45a5-9252-5ad7711354e8,imi:0010000000075BC2"
            },
            {
                "@timestamp": datetime(2017, 10, 29, 21, 49, 12, 672000),
                "level": "INFO",
                "event_id": "0409217a-7120-4afd-8939-49c142749ab8",
                "domain": "INTERNAL",
                "ip": "",
                "method": "[[GET] /scheduler/web/Record/addByProgram]",
                "params": "[{schema=1.0, eventId=crid:~~2F~~2Ftelenet.be~~2Fd3cb0337-2cad-45a5-9252-5ad7711354e8,imi:0010000000075BC2, userId=03e30b70-106c-416b-8243-40fe1e7f981b_be}]",
                "description": "Entity not found (info)",
                "message": "Not found Event entity with programId field equals to crid:~~2F~~2Ftelenet.be~~2Fd3cb0337-2cad-45a5-9252-5ad7711354e8,imi:0010000000075BC2"
            }
        )

    def test_console_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-dvr-console-BS/logs/console_bs_audit.log",
                "message": "27-Oct-2017 14:59:13.207 | ERROR | 746891da-eefe-4b1c-9e78-57c027622c61 | INTERNAL |  | [[GET] /dvr-console/data/sysadmin/status] | [{entriesPageSize=1000, sortDirection=asc, entriesStartIndex=0, sortField=instanceName, count=true}] | Request to service ended with communication error | ConnectException invoking request GET http://be-l-p-obo00331:8086/seachange-import. Exception message: Connection refused (Connection refused)"
            },
            {
                "@timestamp": datetime(2017, 10, 27, 14, 59, 13, 207000),
                "level": "ERROR",
                "event_id": "746891da-eefe-4b1c-9e78-57c027622c61",
                "domain": "INTERNAL",
                "ip": "",
                "method": "[[GET] /dvr-console/data/sysadmin/status]",
                "params": "[{entriesPageSize=1000, sortDirection=asc, entriesStartIndex=0, sortField=instanceName, count=true}]",
                "description": "Request to service ended with communication error",
                "message": "ConnectException invoking request GET http://be-l-p-obo00331:8086/seachange-import. Exception message: Connection refused (Connection refused)"
            }
        )

    def test_scheduler_bs_dev_5_column(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-scheduler-BS/logs/scheduler_bs_dev.log",
                "message": "DEBUG | 28-Oct-2017 16:10:25.987 | b08c03e3-b551-4a83-b2db-44739df6cd00 | qtp670700378-54 | DispatcherServlet - DispatcherServlet with name 'dispatcher' processing GET request for [/scheduler/control/checkAlive]"
            },
            {

                "level": "DEBUG",
                "@timestamp": datetime(2017, 10, 28, 16, 10, 25, 987000),
                "event_id": "b08c03e3-b551-4a83-b2db-44739df6cd00",
                "thread_name": "qtp670700378-54",
                "message": "DispatcherServlet - DispatcherServlet with name 'dispatcher' processing GET request for [/scheduler/control/checkAlive]"
            }
        )

    def test_scheduler_bs_dev_6_column(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-scheduler-BS/logs/scheduler_bs_dev.log",
                "message": "INFO  | 03-Nov-2017 00:56:30.873 | 2e70d186-98a8-44e2-929a-457780b31607 | qtp670700378-183 | RestClient - Sending request to service | Sending request to service: GET http://172.18.253.66:8084/cDVR/control/checkAlive?depth=shallow"
            },
            {

                "level": "INFO",
                "@timestamp": datetime(2017, 11, 3, 0, 56, 30, 873000),
                "event_id": "2e70d186-98a8-44e2-929a-457780b31607",
                "thread_name": "qtp670700378-183",
                "message": "RestClient - Sending request to service",
                "description": "Sending request to service: GET http://172.18.253.66:8084/cDVR/control/checkAlive?depth=shallow"
            }
        )

    def test_cdvr_bs_dev_5_column(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-cDVR-BS/logs/cdvr_bs_dev.log",
                "message": "DEBUG | 28-Oct-2017 16:15:20.33 | 3406f323-48ec-43f2-b3e2-ae6f55e31495 | DefaultQuartzScheduler_Worker-3 | HttpMethodBase - Should NOT close connection, using HTTP/1.1"
            },
            {

                "level": "DEBUG",
                "@timestamp": datetime(2017, 10, 28, 16, 15, 20, 330000),
                "event_id": "3406f323-48ec-43f2-b3e2-ae6f55e31495",
                "thread_name": "DefaultQuartzScheduler_Worker-3",
                "message": "HttpMethodBase - Should NOT close connection, using HTTP/1.1"
            }
        )

    def test_cdvr_bs_dev_6_column(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-cDVR-BS/logs/cdvr_bs_dev.log",
                "message": "INFO  | 03-Nov-2017 00:57:19.367 |  | Timer-1 | RestClient - Sending request to service | Sending request to service: GET http://172.18.253.66:8082/user/data/Configuration?schema=1.0&byParameterName=haActiveInstanceTimeStamp&byServiceName=cdvrBs"
            },
            {

                "level": "INFO",
                "@timestamp": datetime(2017, 11, 3, 0, 57, 19, 367000),
                "event_id": "",
                "thread_name": "Timer-1",
                "message": "RestClient - Sending request to service",
                "description": "Sending request to service: GET http://172.18.253.66:8082/user/data/Configuration?schema=1.0&byParameterName=haActiveInstanceTimeStamp&byServiceName=cdvrBs"
            }
        )
