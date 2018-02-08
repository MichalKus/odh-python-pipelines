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

    def test_vrops_random(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "cpu,name=nlcsapesxp010.csa.internal,res_kind=HostSystem,cpu_id=31 idle_summation=9855.2001953125,used_summation=117.13333129882812,usage_average=0.5806666612625122 1518112552376"
            },
            {
                "group": "cpu",
                "name": "nlcsapesxp010.csa.internal",
                "res_kind": "HostSystem",
                "metrics": {"cpu_id": 31.0, "idle_summation": 9855.2001953125, "used_summation": 117.13333129882812, "usage_average": 0.5806666612625122},
                "timestamp": "1518112552376"
            }
        )

    def test_rare_case(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "net,name=LG-W-P-VDI10028,res_kind=VirtualMachine,interface_id=aggregate\ of\ all\ instances droppedpct=40.50104522705078,packetstxpersec=0.3333333432674408,packetsrxpersec=1.5666667222976685 1518115192381"
            },
            {
                "group": "net",
                "name": "LG-W-P-VDI10028",
                "res_kind": "VirtualMachine",
                "metrics": {"interface_id": "aggregate\\of\\all\\instances", "droppedpct": 40.50104522705078, "packetstxpersec": 0.3333333432674408,
                            "packetsrxpersec": 1.5666667222976685},
                "timestamp": "1518115192381"
            }
        )

    def test_rare_case2(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "availability,name=Likewise\ Service\ Manager,res_kind=vSphere\ SSO\ Likewise\ Service\ Manager resourceavailability=100.0 1518118140000"
            },
            {
                "group": "availability",
                "name": "Likewise\ Service\ Manager",
                "res_kind": "vSphere\\",
                "metrics": {"resourceavailability": 100.0},
                "timestamp": "1518118140000"
            }
        )
