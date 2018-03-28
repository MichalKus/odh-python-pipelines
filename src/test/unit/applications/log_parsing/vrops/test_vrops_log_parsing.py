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
                "message": "1517835494431;name=lg-l-s-uxp00012,kind=VirtualMachine,measurement=disk;usage_average=6.5333333015441895\n"
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
                "message": "1517835494431;name=TN_DEU_PROD_CF_APP_CF_OBO_01_EPG_CF_ACS_DB_01,kind=DistributedVirtualPortgroup,measurement=summary;used_num_ports=4.0,max_num_ports=8.0,ports_down_pct=0.0\n"
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
                "message": "1517835610321;name=vRealize\ Operations\ Manager\ Remote\ Collector-NLCSAPVROPS005C,kind=vC-Ops-Remote-Collector,measurement=resourcelimit;numprocessesmax=192100.0,openfiles=100000.0,numprocesses=192100.0,openfilesmax=100000.0\n"
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
                "message": "1518112552376;name=nlcsapesxp010.csa.internal,kind=HostSystem,measurement=cpu,cpu_id=31;idle_summation=9855.2001953125,used_summation=117.13333129882812,usage_average=0.5806666612625122\n"
            },
            {
                "group": "cpu",
                "name": "nlcsapesxp010.csa.internal",
                "res_kind": "HostSystem",
                "metrics": {"idle_summation": 9855.2001953125, "used_summation": 117.13333129882812, "usage_average": 0.5806666612625122},
                "timestamp": "1518112552376"
            }
        )

    def test_rare_case(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "1518115192381;name=LG-W-P-VDI10028,res_kind=VirtualMachine,interface_id=aggregate\ of\ all\ instances,measurement=net;droppedpct=40.50104522705078,packetstxpersec=0.3333333432674408,packetsrxpersec=1.5666667222976685\n"
            },
            {
                "group": "net",
                "name": "LG-W-P-VDI10028",
                "res_kind": "VirtualMachine",
                "metrics": {"droppedpct": 40.50104522705078, "packetstxpersec": 0.3333333432674408,
                            "packetsrxpersec": 1.5666667222976685},
                "timestamp": "1518115192381"
            }
        )

    def test_rare_case2(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "1518118140000;name=Likewise\ Service\ Manager,res_kind=vSphere\ SSO\ Likewise\ Service\ Manager,measurement=availability;resourceavailability=100.0\n"
            },
            {
                "group": "availability",
                "name": "Likewise\ Service\ Manager",
                "res_kind": "vSphere\ SSO\ Likewise\ Service\ Manager",
                "metrics": {"resourceavailability": 100.0},
                "timestamp": "1518118140000"
            }
        )

    def test_extra_metric(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "1522077287436;identifier=vmnic0,uuid=4f671cc2-06a4-4a24-806c-8fc329feb74a,name=lg-l-p-uxp00007,adapter=VMWARE,kind=VirtualMachine,measurement=net;usage_average=0.0,demand=0.0,bytesrx_average=0.0,packetstxpersec=0.0,transmitted_workload=0.0,transmitted_average=0.0,maxobserved_kbps=25600.0,maxobserved_rx_kbps=12800.0,received_average=0.0,maxobserved_tx_kbps=12800.0,packetsrxpersec=0.0,received_workload=0.0,usage_workload=0.0,bytestx_average=0.0\n"
            },
            {
                "group": "net",
                "name": "lg-l-p-uxp00007",
                "res_kind": "VirtualMachine",
                "metrics": {"usage_average": 0.0, "demand": 0.0, "bytesrx_average": 0.0, "packetstxpersec": 0.0, "transmitted_workload": 0.0, "transmitted_average": 0.0, "maxobserved_kbps": 25600.0, "maxobserved_rx_kbps": 12800.0, "received_average": 0.0, "maxobserved_tx_kbps": 12800.0, "packetsrxpersec": 0.0, "received_workload": 0.0, "usage_workload": 0.0, "bytestx_average": 0.0},
                "timestamp": "1522077287436"
            }
        )

    def test_extra_metric2(self):
        self.assert_parsing(
            {
                "source": "VROPS.log",
                "message": "1522091397392;uuid=7daf42e4-3ccf-4126-a3cd-cf5a3f2f053d,name=lg-l-p-obo00533,adapter=VMWARE,kind=VirtualMachine,measurement=guestfilesystem;freespace_total=74.46806335449219,percentage_total=5.159259796142578,capacity_total=78.5190658569336,usage_total=4.051002502441406\n"
            },
            {
                "group": "guestfilesystem",
                "name": "lg-l-p-obo00533",
                "res_kind": "VirtualMachine",
                "metrics": {"freespace_total": 74.46806335449219, "percentage_total": 5.159259796142578, "capacity_total": 78.5190658569336, "usage_total": 4.051002502441406},
                "timestamp": "1522091397392"
            }
        )
