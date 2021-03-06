from datetime import datetime

from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from applications.log_parsing.traxis_frontend.driver import create_event_creators
from util.configuration import Configuration


class TraxisFrontEndParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_traxis_service_log(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-09-29 11:30:51,656 VERBOSE [184] QueryContext - Normalized query for caching = <Request xmlns=\"urn:eventis:traxisweb:1.0\"><Parameters><Parameter name=\"User-Agent\">recording-service/0.23.0 TentacleClient/5.4.0 Jersey/2.25.1</Parameter><Parameter name=\"language\">en</Parameter></Parameters><ResourcesQuery resourceType=\"event\"><ResourceIds><ResourceId>crid://telenet.be/996d88df-9327-4cfd-84b8-0f61648f42ad,imi:0010000000033E93</ResourceId></ResourceIds><Options><Option type=\"props\">durationinseconds,availabilitystart,availabilityend</Option></Options><SubQueries><SubQuery relationName=\"titles\"><Options><Option type=\"props\">episodename,isadult,name,ordinal,pictures,ratings,minimumage,longsynopsis</Option></Options><SubQueries><SubQuery relationName=\"seriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options><SubQueries><SubQuery relationName=\"parentseriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options></SubQuery></SubQueries></SubQuery></SubQueries></SubQuery></SubQueries></ResourcesQuery></Request>"
            },
            {
                "@timestamp": datetime(2017, 9, 29, 11, 30, 51, 656000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "VERBOSE",
                "thread_name": "184",
                "component": "QueryContext",
                "message": "Normalized query for caching = <Request xmlns=\"urn:eventis:traxisweb:1.0\"><Parameters><Parameter name=\"User-Agent\">recording-service/0.23.0 TentacleClient/5.4.0 Jersey/2.25.1</Parameter><Parameter name=\"language\">en</Parameter></Parameters><ResourcesQuery resourceType=\"event\"><ResourceIds><ResourceId>crid://telenet.be/996d88df-9327-4cfd-84b8-0f61648f42ad,imi:0010000000033E93</ResourceId></ResourceIds><Options><Option type=\"props\">durationinseconds,availabilitystart,availabilityend</Option></Options><SubQueries><SubQuery relationName=\"titles\"><Options><Option type=\"props\">episodename,isadult,name,ordinal,pictures,ratings,minimumage,longsynopsis</Option></Options><SubQueries><SubQuery relationName=\"seriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options><SubQueries><SubQuery relationName=\"parentseriescollection\"><Options><Option type=\"props\">relationordinal,type,name</Option></Options></SubQuery></SubQueries></SubQuery></SubQueries></SubQuery></SubQueries></ResourcesQuery></Request>"
            }
        )

    def test_traxis_service_log_method_duration(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] Executing method 'GetEntitlementForProduct' took '17' ms"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "request-id": "f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab",
                "obo-customer-id": "58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl",
                "method": "GetEntitlementForProduct",
                "duration": "17",
                "message": "[10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] Executing method 'GetEntitlementForProduct' took '17' ms"
            }
        )

    def test_traxis_service_log_method_invoked(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] Method 'GetOffers' invoked with parameters: identity = Eventis.Traxis.BusinessLogicLayer.Identity, productId = crid://eventis.nl/00000000-0000-1000-0008-000100000000, twoLetterIsoLanguageCode = en"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "request-id": "f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab",
                "obo-customer-id": "58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl",
                "method": "GetOffers",
                "identity": "Eventis.Traxis.BusinessLogicLayer.Identity",
                "productId": "crid://eventis.nl/00000000-0000-1000-0008-000100000000",
                "message": "[10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] Method 'GetOffers' invoked with parameters: identity = Eventis.Traxis.BusinessLogicLayer.Identity, productId = crid://eventis.nl/00000000-0000-1000-0008-000100000000, twoLetterIsoLanguageCode = en"
            }
        )

    def test_traxis_service_log_query_metrics_with_requester_id_and_customer_id(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "request-id": "f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab",
                "obo-customer-id": "58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl",
                "response_time_in_milliseconds": 3,
                "queue_time_in_milliseconds": 0,
                "response_length_in_bytes": 2195,
                "cassandra_request_count": 0,
                "external_request_count": 0,
                "resource_evaluation_count": 1,
                "query_metrics": " ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1",
                "message": "[10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            }
        )

    def test_traxis_service_log_query_metrics_with_requester_id_and_customer_id_in_revert_order(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "request-id": "f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab",
                "obo-customer-id": "58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl",
                "response_time_in_milliseconds": 3,
                "queue_time_in_milliseconds": 0,
                "response_length_in_bytes": 2195,
                "cassandra_request_count": 0,
                "external_request_count": 0,
                "resource_evaluation_count": 1,
                "query_metrics": " ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1",
                "message": "[10.64.13.180:39428] [CustomerId = 58a88a40-4d12-11e7-85f5-e5a72ae6734d_nl] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            }
        )

    def test_traxis_service_log_query_metrics_with_only_query_metrics(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "response_time_in_milliseconds": 3,
                "queue_time_in_milliseconds": 0,
                "response_length_in_bytes": 2195,
                "cassandra_request_count": 0,
                "external_request_count": 0,
                "resource_evaluation_count": 1,
                "query_metrics": " ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1",
                "message": "[10.64.13.180:39428] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            }
        )

    def test_traxis_service_log_query_metrics_with_request_id(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "request-id": "f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab",
                "response_time_in_milliseconds": 3,
                "queue_time_in_milliseconds": 0,
                "response_length_in_bytes": 2195,
                "cassandra_request_count": 0,
                "external_request_count": 0,
                "resource_evaluation_count": 1,
                "query_metrics": " ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1",
                "message": "[10.64.13.180:39428] [RequestId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            })

    def test_traxis_service_log_query_metrics_with_customer_id(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:33,468 DEBUG [HTTP worker thread 15] EntitlementManager - [10.64.13.180:39428] [CustomerId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 33, 468000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 15",
                "component": "EntitlementManager",
                "ip": "10.64.13.180:39428",
                "obo-customer-id": "f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab",
                "response_time_in_milliseconds": 3,
                "queue_time_in_milliseconds": 0,
                "response_length_in_bytes": 2195,
                "cassandra_request_count": 0,
                "external_request_count": 0,
                "resource_evaluation_count": 1,
                "query_metrics": " ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1",
                "message": "[10.64.13.180:39428] [CustomerId = f14d79a5-357e-4b6f-bcb7-ed2b00fd63ab] QueryMetrics: ResponseTimeInMilliseconds = 3, QueueTimeInMilliseconds = 0, ResponseLengthInBytes = 2195, CassandraRequestCount = 0, CassandraRequestTotalResponseTimeInMicroseconds = , CassandraRequestAverageResponseTimeInMicroseconds = , ExternalRequestCount = 0, ExternalRequestTotalResponseTimeInMilliseconds = , ResourceEvaluationCount = 1"
            })

    def test_traxis_service_log_cannot_purchase_product(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2017-06-29 16:35:25,640 DEBUG [HTTP worker thread 2] BaseEntitlementManager - [10.64.13.180:39376] [RequestId = 0cc3c8cf-f3b3-4660-9a8c-54e5461106c9] [CustomerId = be73f580-5cc6-11e7-acce-916590705404_nl] Cannot purchase products of type 'Subscription': subscription purchase is not enabled. CustomerId 'be73f580-5cc6-11e7-acce-916590705404_nl', productId 'crid://eventis.nl/00000000-0000-1000-0008-000100000001'"
            },
            {
                "@timestamp": datetime(2017, 6, 29, 16, 35, 25, 640000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "DEBUG",
                "thread_name": "HTTP worker thread 2",
                "component": "BaseEntitlementManager",
                "ip": "10.64.13.180:39376",
                "request-id": "0cc3c8cf-f3b3-4660-9a8c-54e5461106c9",
                "obo-customer-id": "be73f580-5cc6-11e7-acce-916590705404_nl",
                "productId": "crid://eventis.nl/00000000-0000-1000-0008-000100000001",
                "message": "[10.64.13.180:39376] [RequestId = 0cc3c8cf-f3b3-4660-9a8c-54e5461106c9] [CustomerId = be73f580-5cc6-11e7-acce-916590705404_nl] Cannot purchase products of type 'Subscription': subscription purchase is not enabled. CustomerId 'be73f580-5cc6-11e7-acce-916590705404_nl', productId 'crid://eventis.nl/00000000-0000-1000-0008-000100000001'"
            }
        )

    def test_traxis_service_log_x_req_id(self):
        self.assert_parsing(
            {
                "source": "TraxisService.log",
                "message": "2018-04-10 08:52:34,478 WARN  [HTTP worker thread 17] ClassificationSchemeLookupTable - [172.23.41.87, 10.95.97.62] [RequestId = aa0b54f9-8937-4329-9150-0db6343b0cbc] Genre 'urn:tva:metadata:cs:UPCEventGenreCS:2009:46' is not known\nOriginal message from 10.95.96.116:49790\nMethod = Post\nUri = http://traxis-web/traxis/web\nHeaders = \n  x-request-id: a877520b711186cad77077e328fadd82\n  x-application-name: hollow-producer\n  X-B3-TraceId: 11e192c37af66bb4\n  X-B3-SpanId: 11e192c37af66bb4\n  X-B3-Sampled: 0\n  X-Forwarded-For: 172.23.41.87, 10.95.97.62\n  Content-Length: 1391\n  Content-Type: application/xml\n  Accept-Encoding: gzip\n  Host: traxis-web\n  User-Agent: hollow-producer/1.11.01 TentacleClient/5.15.1 Jersey/2.25.1\nBody = \u003c?xml version=\"1.0\" encoding=\"utf-8\"?\u003e\n\u003cRequest xmlns=\"urn:eventis:traxisweb:1.0\"\u003e\n    \u003cParameters\u003e\n        \u003cParameter name=\"Language\"\u003enl\u003c/Parameter\u003e\n    \u003c/Parameters\u003e\n    \u003cRootRelationQuery relationName=\"Titles\"\u003e\n        \u003cOptions\u003e\n            \u003cOption type=\"Paging\"\u003e35000,5000,rc\u003c/Option\u003e\n            \u003cOption type=\"Props\"\u003e\n                Name,\n                Pictures,\n                EpisodeName,\n                SeriesCollection,\n                MinimumAge,\n                DurationInSeconds,\n                SortName,\n                ShortSynopsis,\n                LongSynopsis,\n                ContentProviderId,\n                Categories,\n                IsAdult,\n                ProductionDate,\n                Credits,\n                Genres,\n                AllGenres,\n                ActorsCharacters,\n                DirectorNames,\n                IsTstv,\n                ProductionLocations,\n                StreamingPopularityDay,\n                StreamingPopularityWeek,\n                StreamingPopularityMonth\n            \u003c/Option\u003e\n        \u003c/Options\u003e\n        \u003cSubQueries\u003e\n            \u003cSubQuery relationName=\"SeriesCollection\"\u003e\n                \u003cOptions\u003e\n                    \u003cOption type=\"props\"\u003e\n                        RelationOrdinal\n                    \u003c/Option\u003e\n                \u003c/Options\u003e\n            \u003c/SubQuery\u003e\n        \u003c/SubQueries\u003e\n    \u003c/RootRelationQuery\u003e\n\u003c/Request\u003e\n"
            },
            {
                "@timestamp": datetime(2018, 4, 10, 8, 52, 34, 478000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "WARN",
                "thread_name": "HTTP worker thread 17",
                "component": "ClassificationSchemeLookupTable",
                "ip": "172.23.41.87, 10.95.97.62",
                "request-id": "aa0b54f9-8937-4329-9150-0db6343b0cbc",
                "x-request-id": "a877520b711186cad77077e328fadd82",
                "message": "[172.23.41.87, 10.95.97.62] [RequestId = aa0b54f9-8937-4329-9150-0db6343b0cbc] Genre 'urn:tva:metadata:cs:UPCEventGenreCS:2009:46' is not known\nOriginal message from 10.95.96.116:49790\nMethod = Post\nUri = http://traxis-web/traxis/web\nHeaders = \n  x-request-id: a877520b711186cad77077e328fadd82\n  x-application-name: hollow-producer\n  X-B3-TraceId: 11e192c37af66bb4\n  X-B3-SpanId: 11e192c37af66bb4\n  X-B3-Sampled: 0\n  X-Forwarded-For: 172.23.41.87, 10.95.97.62\n  Content-Length: 1391\n  Content-Type: application/xml\n  Accept-Encoding: gzip\n  Host: traxis-web\n  User-Agent: hollow-producer/1.11.01 TentacleClient/5.15.1 Jersey/2.25.1\nBody = \u003c?xml version=\"1.0\" encoding=\"utf-8\"?\u003e\n\u003cRequest xmlns=\"urn:eventis:traxisweb:1.0\"\u003e\n    \u003cParameters\u003e\n        \u003cParameter name=\"Language\"\u003enl\u003c/Parameter\u003e\n    \u003c/Parameters\u003e\n    \u003cRootRelationQuery relationName=\"Titles\"\u003e\n        \u003cOptions\u003e\n            \u003cOption type=\"Paging\"\u003e35000,5000,rc\u003c/Option\u003e\n            \u003cOption type=\"Props\"\u003e\n                Name,\n                Pictures,\n                EpisodeName,\n                SeriesCollection,\n                MinimumAge,\n                DurationInSeconds,\n                SortName,\n                ShortSynopsis,\n                LongSynopsis,\n                ContentProviderId,\n                Categories,\n                IsAdult,\n                ProductionDate,\n                Credits,\n                Genres,\n                AllGenres,\n                ActorsCharacters,\n                DirectorNames,\n                IsTstv,\n                ProductionLocations,\n                StreamingPopularityDay,\n                StreamingPopularityWeek,\n                StreamingPopularityMonth\n            \u003c/Option\u003e\n        \u003c/Options\u003e\n        \u003cSubQueries\u003e\n            \u003cSubQuery relationName=\"SeriesCollection\"\u003e\n                \u003cOptions\u003e\n                    \u003cOption type=\"props\"\u003e\n                        RelationOrdinal\n                    \u003c/Option\u003e\n                \u003c/Options\u003e\n            \u003c/SubQuery\u003e\n        \u003c/SubQueries\u003e\n    \u003c/RootRelationQuery\u003e\n\u003c/Request\u003e\n"
            }
        )

    def test_traxis_service_error_log(self):
        self.assert_parsing(
            {
                "source": "TraxisServiceError.log",
                "message": "2017-11-07 19:30:19,669 ERROR [169] MachineTimeCheck - [Task = Eventis.Traxis.Service.Ntp.MachineTimeCheck] Eventis.Traxis.Service.ServiceException+NetworkTimeCheckError: NetworkTime error: Time difference between this machine and machine '10.95.97.60' is '17780' ms. This exceeds the configured threshold of '2000' ms"
            },
            {
                "@timestamp": datetime(2017, 11, 07, 19, 30, 19, 669000).replace(tzinfo=timezones["Europe/Amsterdam"]),
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
                "@timestamp": datetime(2017, 9, 29, 14, 41, 58, 832000).replace(tzinfo=timezones["Europe/Amsterdam"]),
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
                "@timestamp": datetime(2017, 11, 14, 14, 54, 35, 666000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "VERBOSE",
                "thread_name": "HTTP worker thread 13",
                "component": "LogManager",
                "message": "[81.82.50.176] [RequestId = 590b040e-a8ae-47ee-969c-58a213999c09] [CustomerId = c9bde815-d03b-46ef-abfe-0b2802116338_be] Executing method 'get_SessionLogger' took '0' ms",
                "duration": "0",
                "ip": "81.82.50.176",
                "method": "get_SessionLogger",
                "obo-customer-id": "c9bde815-d03b-46ef-abfe-0b2802116338_be",
                "request-id": "590b040e-a8ae-47ee-969c-58a213999c09"
            }
        )

    def test_traxis_for_extra_spaces(self):
        self.assert_parsing(
            {
                "source": "TraxisServiceLogManagement.log",
                "message": "2018-01-31 09:20:41,979 INFO  [ResponseCache.Refresh] ResponseCache - [Task = ResponseCache.Refresh] Refreshing '482' queries took '55224' ms"
            },
            {
                "@timestamp": datetime(2018, 1, 31, 9, 20, 41, 979000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "thread_name": "ResponseCache.Refresh",
                "component": "ResponseCache",
                "message": "[Task = ResponseCache.Refresh] Refreshing '482' queries took '55224' ms"

            }
        )
