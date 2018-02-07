from datetime import datetime

from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from applications.log_parsing.vrops.vrops_driver import create_event_creators
from util.configuration import Configuration


class VROPSParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_vrops_single_metric(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "disk,name=lg-l-s-uxp00012,res_kind=VirtualMachine usage_average=6.5333333015441895 1517835494431"
            },
            {
                "group": "disk",
                "name": "lg-l-s-uxp00012",
                "res_kind": "VirtualMachine",
                "metrics": {"usage_average": 6.5333333015441895},
                "timestamp": "1517835494431"
            }
        )

    def test_vrops_multiple_metric(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "summary,name=TN_DEU_PROD_CF_APP_CF_OBO_01_EPG_CF_ACS_DB_01,res_kind=DistributedVirtualPortgroup used_num_ports=4.0,max_num_ports=8.0,ports_down_pct=0.0 1517835494431"
            },
            {
                "group": "summary",
                "name": "TN_DEU_PROD_CF_APP_CF_OBO_01_EPG_CF_ACS_DB_01",
                "res_kind": "DistributedVirtualPortgroup",
                "metrics": {"used_num_ports": 4.0, "max_num_ports": 8.0, "ports_down_pct":0.0},
                "timestamp": "1517835494431"
            }
        )

    def test_vrops_with_gap(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "resourcelimit,name=vRealize\ Operations\ Manager\ Remote\ Collector-NLCSAPVROPS005C,res_kind=vC-Ops-Remote-Collector numprocessesmax=192100.0,openfiles=100000.0,numprocesses=192100.0,openfilesmax=100000.0 1517835610321"
            },
            {
                "group": "resourcelimit",
                "name": "vRealize\\ Operations\\ Manager\\ Remote\\ Collector-NLCSAPVROPS005C",
                "res_kind": "vC-Ops-Remote-Collector",
                "metrics": {"numprocessesmax": 192100.0, "openfiles": 100000.0, "numprocesses": 192100.0, "openfilesmax": 100000.0},
                "timestamp": "1517835610321"
            }
        )
