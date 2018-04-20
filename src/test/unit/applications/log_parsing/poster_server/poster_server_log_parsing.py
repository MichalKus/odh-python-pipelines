from datetime import datetime

from applications.log_parsing.poster_server.driver import create_event_creators
from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration


class PosterServerMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_parse_poster_server_log(self):
        self.assert_parsing(
            {
                'source': 'PosterServer.log',
                'message': '2017-09-04 12:30:33,740   WARN    Config OverlaysDirectory not set'
            },
            {
                '@timestamp': datetime(2017, 9, 4, 12, 30, 33, 740000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                'level': 'WARN',
                'module': 'Config',
                'message': 'OverlaysDirectory not set'
            }
        )

    def test_parse_poster_server_log_crid(self):
        self.assert_parsing(
            {
                'source': 'PosterServer.log',
                'message': '2018-04-03 13:42:01,986 INFO  ImageTransformer               Queueing the generation of (D:\\PosterServer\\.resized\\OndemandImages\\BE\\PI\\crid~~3A~~2F~~2Ftelenet.be~~2F21a46636-c0e8-4865-a752-94a1ce45eda2\\120x0_Box_96x96dpi_Jpg\\95d0cf239b261388c554b3fae1d65e78.jpg)'
            },
            {
                '@timestamp': datetime(2018, 4, 3, 13, 42, 1, 986000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                'level': 'INFO',
                'module': 'ImageTransformer',
                'message': 'Queueing the generation of (D:\\PosterServer\\.resized\\OndemandImages\\BE\\PI\\crid~~3A~~2F~~2Ftelenet.be~~2F21a46636-c0e8-4865-a752-94a1ce45eda2\\120x0_Box_96x96dpi_Jpg\\95d0cf239b261388c554b3fae1d65e78.jpg)',
                'crid': 'crid~~3A~~2F~~2Ftelenet.be~~2F21a46636-c0e8-4865-a752-94a1ce45eda2'
            }
        )

    def test_parse_poster_server_error_log(self):
        self.assert_parsing(
            {
                'source': 'PosterServer.Error.log',
                'message': '2017-09-04 12:30:33,740     ERROR       ResizerModule File not found (EventImages/1.jpg)'
            },
            {
                '@timestamp': datetime(2017, 9, 4, 12, 30, 33, 740000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                'level': 'ERROR',
                'module': 'ResizerModule',
                'message': 'File not found (EventImages/1.jpg)'
            }
        )
