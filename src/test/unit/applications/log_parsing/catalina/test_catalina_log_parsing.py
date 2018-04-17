from datetime import datetime

from dateutil.tz import tzutc

from applications.log_parsing.catalina.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration


class CatalinaParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_catalina(self):
        self.assert_parsing(
            {
                "source": "/penthera/logs/mongodb/mongo.log",
                "message": "2018-04-11T07:49:19.072+0000 I NETWORK  [conn1564572] end connection 172.16.145.9:54914 (1141 connections now open)"
            },
            {
                "@timestamp": datetime(2018, 4, 11, 7, 49, 19, 72000, ).replace(tzinfo=tzutc()),
                "level": "I",
                "event_type": "NETWORK",
                "thread": "conn1564572",
                "message": "end connection 172.16.145.9:54914 (1141 connections now open)",
            }
        )
