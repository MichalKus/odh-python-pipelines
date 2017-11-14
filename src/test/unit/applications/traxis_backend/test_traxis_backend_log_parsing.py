from datetime import datetime

from applications.log_parsing.traxis.traxis_backend import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class TraxisBackendParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_traxis_backend(self):
        test_cases = [(
            {
                "source": "TraxisService.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-10-03 15:47:00,109 DEBUG [149] WcfHelper - Wcf call 'GetPromotionRuleLogData' took '10' ms"
            }, {
                "@timestamp": datetime(2017, 10, 3, 15, 47, 0, 109000),
                "level": "DEBUG",
                "message": "WcfHelper - Wcf call 'GetPromotionRuleLogData' took '10' ms"
            }
        ), (
            {
                "source": "TraxisServiceLogManagement.log",
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_gen_v1",
                "message": "2017-10-03 15:54:30,103 DEBUG [113] Logger`1 - Retrieving '0' rows took '7' ms. Pages processed = '2'"
            },
            {
                "@timestamp": datetime(2017, 10, 3, 15, 54, 30, 103000),
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
                "@timestamp": datetime(2017, 9, 26, 16, 58, 57, 79000),
                "level": "INFO",
                "message": "DistributedScheduler - [Task = DistributedScheduler.Slave] Assignments in database too old '2017-09-26T15:58:33Z'. Waiting until a more recent version is written by the master"
            }
        ), (
            {
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_backend_log_err_v1",
                "message": "2017-09-27 11:35:20,711 ERROR [118] MachineTimeCheck - [Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.59' is '2848' ms. This exceeds the configured threshold of '2000' ms"
            },
            {
                "@timestamp": datetime(2017, 9, 27, 11, 35, 20, 711000),
                "level": "ERROR",
                "message": "MachineTimeCheck - [Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.59' is '2848' ms. This exceeds the configured threshold of '2000' ms"
            }
        )]

        for test_message, parsed_message in test_cases:
            self.assert_parsing(test_message, parsed_message)
