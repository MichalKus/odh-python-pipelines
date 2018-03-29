from datetime import datetime

from applications.log_parsing.f5.driver import create_event_creators
from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration


class F5MessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_f5_message_log_parsing(self):
        self.assert_parsing(
            {
                'message': 'Hostname="nl-srk03a-fe-vlb-lgcf12.aorta.net",Entity="Traffic",AggrInterval="10",EOCTimestamp="1521457813",RequestStartTimestamp="1521457813",ResponseStartTimestamp="1521457813",AVRProfileName="/Common/PFL_ANALYTICS_HTTP_SP_OBO",ApplicationName="<Unassigned>",VSName="/Common/OBO_ODH_DEU_443",POOLIP="2001:db8:1:0:ef21:0:1b1c:fe02",POOLIPRouteDomain="0",POOLPort="0",URLString="/report",ClientIP="213.46.252.136",ClientIPRouteDomain="0",ClientPort="50634",UserAgentString="",MethodString="POST",ResponseCode="0",GeoCode="NL",ServerLatency="0",RequestSize="5170",ResponseSize="0",RequestHeader="POST /report HTTP/1.1\\r\\nHost: obousage.prod.de.dmdsdp.com\\r\\nContent-Length: 4988\\r\\ncontent-type: application/json\\r\\nX-Forwarded-For: 213.46.252.136\\r\\nX-dev: 3C36E4-EOSSTB-003392565903\\r\\n\\r\\n",RequestHeaderTruncated="0",ResponseHeaderTruncated="0",RequestPayloadTruncated="0",ResponsePayloadTruncated="0",MitigatedByDoSL7="0"'
            },
            {
                "@timestamp": datetime(2018, 3, 19, 11, 10, 13).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "aggr_interval": "10",
                "application_name": "<Unassigned>",
                "avr_profile_name": "/Common/PFL_ANALYTICS_HTTP_SP_OBO",
                "client_ip": "213.46.252.136",
                "client_ip_route_domain": "0",
                "client_port": "50634",
                "content-length": "4988",
                "content-type": "application/json",
                "entity": "Traffic",
                "eoc_timestamp": "1521457813",
                "geo_code": "NL",
                "host": "obousage.prod.de.dmdsdp.com",
                "hostname": "nl-srk03a-fe-vlb-lgcf12.aorta.net",
                "method_string": "POST",
                "mitigated_by_do_sl7": "0",
                "pool_port": "0",
                "poolip": "2001:db8:1:0:ef21:0:1b1c:fe02",
                "poolip_route_domain": "0",
                "request_header": 'POST /report HTTP/1.1\\r\\nHost: obousage.prod.de.dmdsdp.com\\r\\nContent-Length: 4988\\r\\ncontent-type: application/json\\r\\nX-Forwarded-For: 213.46.252.136\\r\\nX-dev: 3C36E4-EOSSTB-003392565903\\r\\n\\r\\n',
                "request_header_truncated": "0",
                "request_payload_truncated": "0",
                "request_size": "5170",
                "request_start_timestamp": "1521457813",
                "response_code": "0",
                "response_header_truncated": "0",
                "response_payload_truncated": "0",
                "response_size": "0",
                "response_start_timestamp": "1521457813",
                "server_latency": "0",
                "url_string": "/report",
                "vs_name": "/Common/OBO_ODH_DEU_443",
                "x-dev": "3C36E4-EOSSTB-003392565903",
                "x-forwarded-for": "213.46.252.136"
            }
        )
