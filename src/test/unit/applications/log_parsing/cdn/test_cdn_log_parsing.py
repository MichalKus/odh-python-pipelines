from datetime import datetime

from applications.log_parsing.cdn.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration
from common.log_parsing.timezone_metadata import timezones


class CdnParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_cdn(self):
        self.assert_parsing(
            {
                "message": "ac4.vt2ind1d1.cdn\t2018-03-21\t04:53:39\t0.001\t52.67.236.131\t34880\texternal\tg\t-\t-\tGET\thttp://upc-cl-37.live.horizon.tv/ss/Paramount.isml/QualityLevels(96000)/Fragments(audio101_spa=184186112755222)\t1.1\t\"Apache-HttpClient/4.5.3 (Java/1.8.0_141)\"\t-\t-\t-\t200\tCACHE_MEM_HIT\t26014\t25341\t0\t186.156.250.12\t0\t-\t126\t0\t0\t-\t-\tWP:7e00000000000000\t-\t4ae1bc\tu--"
            },
            {
                "s_dns": "ac4.vt2ind1d1.cdn",
                "@timestamp": datetime(2018, 3, 21, 4, 53, 39).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "x_duration": "0.001",
                "c_ip": "52.67.236.131",
                "c_port": "34880",
                "c_vx_zone": "external",
                "c_vx_gloc": "g",
                "unknown_field1": "-",
                "unknown_field2": "-",
                "cs_method": "GET",
                "cs_uri": "http://upc-cl-37.live.horizon.tv/ss/Paramount.isml/QualityLevels(96000)/Fragments(audio101_spa=184186112755222)",
                "cs_version": "1.1",
                "cs_user_agent": "\"Apache-HttpClient/4.5.3 (Java/1.8.0_141)\"",
                "cs_refer": "-",
                "cs_cookie": "-",
                "cs_range": "-",
                "cs_status": "200",
                "s_cache_status": "CACHE_MEM_HIT",
                "sc_bytes": "26014",
                "sc_stream_bytes": "25341",
                "sc_dscp": "0",
                "s_ip": "186.156.250.12",
                "s_vx_rate": "0",
                "s_vx_rate_status": "-",
                "s_vx_serial": "126",
                "rs_stream_bytes": "0",
                "rs_bytes": "0",
                "cs_vx_token": "-",
                "sc_vx_download_rate": "-",
                "x_protohash": "WP:7e00000000000000",
                "additional_headers": "-",
                "unknown_field3": "4ae1bc",
                "unknown_field4": "u--",
            }
        )
