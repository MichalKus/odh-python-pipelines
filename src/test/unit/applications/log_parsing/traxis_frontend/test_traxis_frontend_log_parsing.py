from datetime import datetime

from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from applications.log_parsing.traxis_frontend.driver import create_event_creators


class TraxisFrontEndParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_traxis_service_log(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-09-29 11:30:51,656 VERBOSE [184] QueryContext - Normalized query for caching = <Request xmlns=\"urn:eventis:traxisweb:1.0\"><Parameters><Parameter name=\"User-Agent\">recording-service/0.23.0 TentacleClient/5.4.0 Jersey/2.25.1</Parameter><Parameter name=\"language\">en</Parameter></Parameters><ResourcesQuery resourceType=\"event\"><ResourceIds><ResourceId>crid://telenet.be/996d88df-9327-4cfd-84b8-0f61648f42ad,imi:0010000000033E93</ResourceId></ResourceIds><Options><Option type=\"props\">durationinseconds,availabilitystart,availabilityend</Option></Options><SubQueries><SubQuery relationName=\"titles\"><Options><Option type=\"props\">episodename,isadult,name,ordinal,pictures,ratings,minimumage,longsynopsis</Option></Options><SubQueries><SubQuery relationName=\"seriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options><SubQueries><SubQuery relationName=\"parentseriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options></SubQuery></SubQueries></SubQuery></SubQueries></SubQuery></SubQueries></ResourcesQuery></Request>"
            },
            {
                "@timestamp": datetime(2017, 9, 29, 11, 30, 51, 656000),
                "level": "VERBOSE",
                "thread_name": "184",
                "component": "QueryContext",
                "message": "Normalized query for caching = <Request xmlns=\"urn:eventis:traxisweb:1.0\"><Parameters><Parameter name=\"User-Agent\">recording-service/0.23.0 TentacleClient/5.4.0 Jersey/2.25.1</Parameter><Parameter name=\"language\">en</Parameter></Parameters><ResourcesQuery resourceType=\"event\"><ResourceIds><ResourceId>crid://telenet.be/996d88df-9327-4cfd-84b8-0f61648f42ad,imi:0010000000033E93</ResourceId></ResourceIds><Options><Option type=\"props\">durationinseconds,availabilitystart,availabilityend</Option></Options><SubQueries><SubQuery relationName=\"titles\"><Options><Option type=\"props\">episodename,isadult,name,ordinal,pictures,ratings,minimumage,longsynopsis</Option></Options><SubQueries><SubQuery relationName=\"seriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options><SubQueries><SubQuery relationName=\"parentseriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options></SubQuery></SubQueries></SubQuery></SubQueries></SubQuery></SubQueries></ResourcesQuery></Request>"
            }
        )

    def test_traxis_service_error_log(self):
        self.assert_parsing(
            {
                "source": "TraxisServiceError.log",
                "message": "2017-11-07 19:30:19,669 ERROR [169] MachineTimeCheck - [Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.60' is '17780' ms. This exceeds the configured threshold of '2000' ms"
            },
            {
                "@timestamp": datetime(2017, 11, 07, 19, 30, 19, 669000),
                "level": "ERROR",
                "thread_name": "169",
                "component": "MachineTimeCheck",
                "message": "[Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.60' is '17780' ms. This exceeds the configured threshold of '2000' ms"

            }
        )

    def test_traxis_service_scheduler_log(self):
        self.assert_parsing(
            {
                "source": "TraxisServiceDistributedScheduler.log",
                "message": "2017-09-29 14:41:58,832 DEBUG [71] DistributedScheduler - [Task = DistributedScheduler.Master] Machines in up state: BE-W-P-OBO00170, BE-W-P-OBO00173, BE-W-P-OBO00174"
            },
            {
                "@timestamp": datetime(2017, 9, 29, 14, 41, 58, 832000),
                "level": "DEBUG",
                "thread_name": "71",
                "component": "DistributedScheduler",
                "message": "[Task = DistributedScheduler.Master] Machines in up state: BE-W-P-OBO00170, BE-W-P-OBO00173, BE-W-P-OBO00174"

            }
        )

    def test_traxis_service_management_log(self):
        self.assert_parsing(
            {
                "source": "TraxisServiceLogManagement.log",
                "message": "2017-11-14 14:54:35,666 VERBOSE [HTTP worker thread 13] LogManager - [81.82.50.176] [RequestId = 590b040e-a8ae-47ee-969c-58a213999c09] [CustomerId = c9bde815-d03b-46ef-abfe-0b2802116338_be] Executing method 'get_SessionLogger' took '0' ms"
            },
            {
                "@timestamp": datetime(2017, 11, 14, 14, 54, 35, 666000),
                "level": "VERBOSE",
                "thread_name": "HTTP worker thread 13",
                "component": "LogManager",
                "message": "[81.82.50.176] [RequestId = 590b040e-a8ae-47ee-969c-58a213999c09] [CustomerId = c9bde815-d03b-46ef-abfe-0b2802116338_be] Executing method 'get_SessionLogger' took '0' ms"

            }
        )
