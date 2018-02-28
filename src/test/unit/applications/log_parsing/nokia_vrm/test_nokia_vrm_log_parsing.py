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
