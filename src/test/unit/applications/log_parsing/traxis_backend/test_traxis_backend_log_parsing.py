from datetime import datetime

from applications.log_parsing.traxis_backend.driver import create_event_creators
from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration


class TraxisBackendParsingTestCase(BaseMultipleMessageParsingTestCase):
    """
    A unit test to check a work of event creators for the Traxis Backend log parsing job.
    """

    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_traxis_backend(self):
        test_cases = [(
            {
                "source": "TraxisService.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-10-03 15:47:00,109 DEBUG [149] WcfHelper - "
                           "Wcf call 'GetPromotionRuleLogData' took '10' ms"
            }, {
                "@timestamp": datetime(2017, 10, 3, 15, 47, 0, 109000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "message": "WcfHelper - Wcf call 'GetPromotionRuleLogData' took '10' ms"
            }
        ), (
            {
                "source": "TraxisService.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-07-10 00:37:23,829 INFO  [HTTP worker thread 18] OnlineTvaIngest - "
                           "[172.30.182.17:55347] [RequestId = 5a903fc1-02a5-4424-8c16-4078f83df6c4] "
                           "Tva notification received"
            }, {
                "@timestamp": datetime(2017, 7, 10, 0, 37, 23, 829000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "OnlineTvaIngest - [172.30.182.17:55347] [RequestId = 5a903fc1-02a5-4424-8c16-4078f83df6c4] "
                           "Tva notification received",
                "activity": "OnlineTvaIngest",
                "requestId": "5a903fc1-02a5-4424-8c16-4078f83df6c4"
            }
        ), (
            {
                "source": "TraxisService.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-07-10 00:37:26,417 INFO  [105] TvaManager - [Task = Notification of type "
                           "'TvaIngestCompleted'] Loading tva version '233.1221' took '314' ms"
            }, {
                "@timestamp": datetime(2017, 7, 10, 0, 37, 26, 417000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "TvaManager - [Task = Notification of type 'TvaIngestCompleted'] "
                           "Loading tva version '233.1221' took '314' ms",
                "activity": "TvaManager",
                "task": "Notification of type 'TvaIngestCompleted'",
                "duration": 314
            }
        ), (
            {
                "source": "TraxisService.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-07-10 00:39:25,522 INFO  [160] ParsingContext - [Task = TvaManagementExpirationCheck] "
                           "Tva ingest completed, duration = 1478 ms, new version = 233.1222, entities set = 2, "
                           "deleted = 1 (total = 425429), Event set = 0, deleted = 1 (total = 200840), Channel "
                           "set = 1, deleted = 0 (total = 488), Title set = 1, deleted = 0 (total = 191667)"
            }, {
                "@timestamp": datetime(2017, 7, 10, 0, 39, 25, 522000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "ParsingContext - [Task = TvaManagementExpirationCheck] Tva ingest completed, duration = "
                           "1478 ms, new version = 233.1222, entities set = 2, deleted = 1 (total = 425429), Event "
                           "set = 0, deleted = 1 (total = 200840), Channel set = 1, deleted = 0 (total = 488), "
                           "Title set = 1, deleted = 0 (total = 191667)",
                "activity": "ParsingContext",
                "task": "TvaManagementExpirationCheck",
                "duration": 1478
            }
        ), (
            {
                "source": "TraxisService.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-07-10 00:40:25,641 INFO  [149] ParsingContext - [Task = TvaManagementExpirationCheck] "
                           "Number of write actions queued = 125. Action took 110 ms"
            }, {
                "@timestamp": datetime(2017, 7, 10, 0, 40, 25, 641000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "ParsingContext - [Task = TvaManagementExpirationCheck] "
                           "Number of write actions queued = 125. Action took 110 ms",
                "activity": "ParsingContext",
                "task": "TvaManagementExpirationCheck",
                "duration": 110
            }
        ), (
            {
                "source": "TraxisServiceLogManagement.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-10-03 15:54:30,103 DEBUG [113] Logger`1 - Retrieving '0' rows took '7' ms. Pages processed = '2'"
            },
            {
                "@timestamp": datetime(2017, 10, 3, 15, 54, 30, 103000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "message": "Logger`1 - Retrieving '0' rows took '7' ms. Pages processed = '2'"
            }
        ), (
            {
                "source": "TraxisServiceDistributedScheduler.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-09-26 16:58:57,079 INFO [61] DistributedScheduler - [Task = DistributedScheduler.Slave] Assignments in database too old '2017-09-26T15:58:33Z'. Waiting until a more recent version is written by the master"
            },
            {
                "@timestamp": datetime(2017, 9, 26, 16, 58, 57, 79000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "message": "DistributedScheduler - [Task = DistributedScheduler.Slave] Assignments in database too old '2017-09-26T15:58:33Z'. Waiting until a more recent version is written by the master"
            }
        ), (
            {
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_err_v1",
                "message": "2017-09-27 11:35:20,711 ERROR [118] MachineTimeCheck - [Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.59' is '2848' ms. This exceeds the configured threshold of '2000' ms"
            },
            {
                "@timestamp": datetime(2017, 9, 27, 11, 35, 20, 711000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "message": "MachineTimeCheck - [Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.59' is '2848' ms. This exceeds the configured threshold of '2000' ms"
            }
        )]

        for test_message, parsed_message in test_cases:
            self.assert_parsing(test_message, parsed_message)
