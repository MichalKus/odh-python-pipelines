from datetime import datetime

from applications.log_parsing.poster_server.driver import create_event_creators
from common.log_parsing.metadata import ParsingException
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class PosterServerMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_parse_poster_server_log(self):
        self.assert_parsing(
            {
                'source': 'PosterServer.log',
                'message': '2017-09-04 12:30:33.740   WARN    Config OverlaysDirectory not set'
            },
            {
                '@timestamp': datetime(2017, 9, 4, 12, 30, 33, 740000),
                'level': 'WARN',
                'module': 'Config',
                'message': 'OverlaysDirectory not set'
            }
        )

    def test_parse_poster_server_error_log(self):
        self.assert_parsing(
            {
                'source': 'PosterServer.Error.log',
                'message': '2017-09-04 12:30:33.740     ERROR       ResizerModule File not found (EventImages/1.jpg)'
            },
            {
                '@timestamp': datetime(2017, 9, 4, 12, 30, 33, 740000),
                'level': 'ERROR',
                'module': 'ResizerModule',
                'message': 'File not found (EventImages/1.jpg)'
            }
        )

    def test_parse_fails(self):
        row = {
            'source': 'PosterServer.Error.log',
            'message': '2017-09- 04 12:30:33.740     ERROR       ResizerModule File not found (EventImages/1.jpg)'
        }
        with self.assertRaises(ParsingException):
            self.event_creators.get_parsing_context(row).event_creator.create(row)
