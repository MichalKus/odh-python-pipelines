from datetime import datetime

from applications.log_parsing.nokia_vrm.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class NokiaVrmMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()
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
