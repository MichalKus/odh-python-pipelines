import json
from datetime import datetime

from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from applications.log_parsing.uservices.driver import create_event_creators
from util.configuration import Configuration


class UServicesParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_uservices_log(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "gateway",
                        "x-request-id": "2bb963ae-d492-41a2-b8f8-a94d8810248a",
                        "stack": "gateway-ch",
                        "offset": 87181738,
                        "log": "",
                        "json_error": "Key 'log' not found",
                        "input_type": "log",
                        "time_local": "21/Mar/2018:14:16:05 +0100",
                        "source": "/app/nginx/access.log",
                        "x-forwarded-for": "172.23.41.84",
                        "message": "172.16.96.133 - - [21/Mar/2018:14:16:05 +0100] \"GET /reng/RE/REController.do?contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true HTTP/1.1\" 200 636",
                        "type": "uservices_log_in",
                        "x-dev": "-",
                        "@timestamp": "2018-03-21T13:16:06.753Z",
                        "x-auth-id": "-",
                        "beat": {
                            "hostname": "lg-l-p-obo00579",
                            "name": "lg-l-p-obo00579",
                            "version": "5.6.5"
                        },
                        "host": "lg-l-p-obo00579",
                        "@version": "1",
                        "http": {
                            "referer": "-",
                            "request": "/reng/RE/REController.do?contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true",
                            "method": "GET",
                            "local_port": "80",
                            "useragent": "discovery-service/0.25.1 TentacleClient/5.10.0 Jersey/2.25.1",
                            "proxy_to": "reng:80",
                            "duration": 0.018,
                            "protocol": "HTTP/1.1",
                            "urlpath": "/reng/RE/REController.do",
                            "urlquery": "contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true",
                            "bytes": 636,
                            "clientip": "172.16.96.133",
                            "domain": "gateway",
                            "status": 200
                        },
                        "upstream_response_time": "0.018",
                        "x-cus": "-"
                    })
            },
            {
                "app": "gateway",
                "x-request-id": "2bb963ae-d492-41a2-b8f8-a94d8810248a",
                "stack": "gateway-ch",
                "offset": 87181738,
                "log": "",
                "json_error": "Key 'log' not found",
                "input_type": "log",
                "time_local": "21/Mar/2018:14:16:05 +0100",
                "source": "/app/nginx/access.log",
                "x-forwarded-for": "172.23.41.84",
                "message": "172.16.96.133 - - [21/Mar/2018:14:16:05 +0100] \"GET /reng/RE/REController.do?contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true HTTP/1.1\" 200 636",
                "type": "uservices_log_in",
                "x-dev": "-",
                "@timestamp": datetime(2018, 3, 21, 13, 16, 6, 753000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "x-auth-id": "-",
                "beat": {
                    "hostname": "lg-l-p-obo00579",
                    "name": "lg-l-p-obo00579",
                    "version": "5.6.5"
                },
                "host": "lg-l-p-obo00579",
                "@version": "1",
                "http_referer": "-",
                "http_request": "/reng/RE/REController.do?contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true",
                "http_method": "GET",
                "http_local_port": "80",
                "http_useragent": "discovery-service/0.25.1 TentacleClient/5.10.0 Jersey/2.25.1",
                "http_proxy_to": "reng:80",
                "http_duration": 0.018,
                "http_protocol": "HTTP/1.1",
                "http_urlpath": "/reng/RE/REController.do",
                "http_urlquery": "contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true",
                "content_source_id": "1",
                "client_type": "399",
                "method": "lgiAdaptiveSearch",
                "subscriber_id": "6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile",
                "clean_subscriber_id": "6b10a120-ce10-11e7-8a8a-5fa419531185",
                "term": "News",
                "lgi_content_item_instance_id": "%5B1%5Dimi%3A00100000001950EC",
                "clean_lgi_content_item_instance_id": "00100000001950EC",
                "content_item_id": "%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7",
                "clean_content_item_id": "fcdf57b1-1cc4-4e13-ad34-c348b26315c7",
                "start_results": "0",
                "max_results": "1",
                "apply_marketing_bias": "true",
                "filter_vod_available_now": "true",
                "http_bytes": 636,
                "http_clientip": "172.16.96.133",
                "http_domain": "gateway",
                "http_status": 200,
                "upstream_response_time": "0.018",
                "x-cus": "-"
            }
        )

    def test_uservices_log_with_content_item_id_that_does_not_contain_telenet_be(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "http": {
                            "urlquery": "contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true",
                        },
                    })
            },
            {

                "http_urlquery": "contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile&term=News&lgiContentItemInstanceId=%5B1%5Dimi%3A00100000001950EC&contentItemId=%5B2%5Dcrid%3A%2F%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7&startResults=0&maxResults=1&applyMarketingBias=true&filterVodAvailableNow=true",
                "content_source_id": "1",
                "client_type": "399",
                "method": "lgiAdaptiveSearch",
                "subscriber_id": "6b10a120-ce10-11e7-8a8a-5fa419531185_ch%23MasterProfile",
                "clean_subscriber_id": "6b10a120-ce10-11e7-8a8a-5fa419531185",
                "term": "News",
                "lgi_content_item_instance_id": "%5B1%5Dimi%3A00100000001950EC",
                "clean_lgi_content_item_instance_id": "00100000001950EC",
                "content_item_id": "%5B2%5Dcrid%3A%2F%2Ffcdf57b1-1cc4-4e13-ad34-c348b26315c7",
                "start_results": "0",
                "max_results": "1",
                "apply_marketing_bias": "true",
                "filter_vod_available_now": "true",
            }
        )

    def test_uservices_log_parse_and_clean_subscriber_id(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "http": {
                            "urlquery": "subscriberId=1e1e8640-17b0-11e8-93dc-e71e3a262bec_de%23MasterProfile",
                        },
                    })
            },
            {

                "http_urlquery": "subscriberId=1e1e8640-17b0-11e8-93dc-e71e3a262bec_de%23MasterProfile",
                "subscriber_id": "1e1e8640-17b0-11e8-93dc-e71e3a262bec_de%23MasterProfile",
                "clean_subscriber_id": "1e1e8640-17b0-11e8-93dc-e71e3a262bec"
            }
        )

    def test_uservices_log_parse_and_clean_content_item_id_with_telenet_be(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "http": {
                            "urlquery": "contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Fdc8f839f-dd41-49bf-842f-e6208e08ee02",
                        }
                    })
            },
            {

                "http_urlquery": "contentItemId=%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Fdc8f839f-dd41-49bf-842f-e6208e08ee02",
                "content_item_id": "%5B2%5Dcrid%3A%2F%2Ftelenet.be%2Fdc8f839f-dd41-49bf-842f-e6208e08ee02",
                "clean_content_item_id": "dc8f839f-dd41-49bf-842f-e6208e08ee02"
            }
        )

    def test_uservices_log_parse_content_item_id_without_telenet_be(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "http": {
                            "urlquery": "contentItemId=crid%3A%2F%2Fbds.tv%2F134877645",
                        },
                    })
            },
            {

                "http_urlquery": "contentItemId=crid%3A%2F%2Fbds.tv%2F134877645",
                "content_item_id": "crid%3A%2F%2Fbds.tv%2F134877645",
            }
        )

    def test_uservices_log_parse_and_clean_lgi_content_item_instance_id(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "http": {
                            "urlquery": "lgiContentItemInstanceId=%5B1%5Dimi%3A0010000000185C2C",
                        }
                    })
            },
            {

                "http_urlquery": "lgiContentItemInstanceId=%5B1%5Dimi%3A0010000000185C2C",
                "lgi_content_item_instance_id": "%5B1%5Dimi%3A0010000000185C2C",
                "clean_lgi_content_item_instance_id": "0010000000185C2C"
            }
        )

    def test_extract_bookings_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "recording-service",
                        "header": {
                            "x-original-uri": "/recording-service/customers/1a2a5370-dc2d-11e7-a33c-8ffe245e867f_nl/bookings?language=nl&limit=2147483647&isAdult=false",
                        }
                    })
            },
            {
                "app": "recording-service",
                "header_x-original-uri": "/recording-service/customers/1a2a5370-dc2d-11e7-a33c-8ffe245e867f_nl/bookings?language=nl&limit=2147483647&isAdult=false",
                "api_method": "bookings"
            }
        )

    def test_extract_recordings_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "recording-service",
                        "header": {
                            "x-original-uri": "/recording-service/customers/93071743-eb84-4f01-9d92-53d8ad503789_be/recordings/contextual?language=en",
                        }
                    })
            },
            {
                "app": "recording-service",
                "header_x-original-uri": "/recording-service/customers/93071743-eb84-4f01-9d92-53d8ad503789_be/recordings/contextual?language=en",
                "api_method": "recordings"
            }
        )

    def test_extract_history_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "purchase-service",
                        "header": {
                            "x-original-uri": "/purchase-service/customers/c6ffb300-f529-11e7-93dc-e71e3a262bec_de/history",
                        }
                    })
            },
            {
                "app": "purchase-service",
                "header_x-original-uri": "/purchase-service/customers/c6ffb300-f529-11e7-93dc-e71e3a262bec_de/history",
                "api_method": "history"
            }
        )

    def test_extract_entitlements_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "purchase-service",
                        "header": {
                            "x-original-uri": "/purchase-service/customers/c6ffb300-f529-11e7-93dc-e71e3a262bec_de/entitlements/3C36E4-EOSSTB-003398520902",
                        }
                    })
            },
            {
                "app": "purchase-service",
                "header_x-original-uri": "/purchase-service/customers/c6ffb300-f529-11e7-93dc-e71e3a262bec_de/entitlements/3C36E4-EOSSTB-003398520902",
                "api_method": "entitlements"
            }
        )

    def test_extract_contextualvod_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "vod-service",
                        "header": {
                            "x-original-uri": "/vod-service/v2/contextualvod/omw_playmore_nl?country=be&language=nl&profileId=86e4425d-9405-4a60-9b07-d3b105d9c27e_be~~23MasterProfile&optIn=true",
                        }
                    })
            },
            {
                "app": "vod-service",
                "header_x-original-uri": "/vod-service/v2/contextualvod/omw_playmore_nl?country=be&language=nl&profileId=86e4425d-9405-4a60-9b07-d3b105d9c27e_be~~23MasterProfile&optIn=true",
                "api_method": "contextualvod"
            }
        )

    def test_extract_detailscreen_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "vod-service",
                        "header": {
                            "x-original-uri": "/vod-service/v2/detailscreen/crid:~~2F~~2Ftelenet.be~~2F486e1365-35fd-46b1-ac04-d42e173e7dfa?country=be&language=nl&profileId=12c41366-0a93-4e54-b1b6-efcf22c132b9_be~~23MasterProfile&t"
                        }
                    })
            },
            {
                "app": "vod-service",
                "header_x-original-uri": "/vod-service/v2/detailscreen/crid:~~2F~~2Ftelenet.be~~2F486e1365-35fd-46b1-ac04-d42e173e7dfa?country=be&language=nl&profileId=12c41366-0a93-4e54-b1b6-efcf22c132b9_be~~23MasterProfile&t",
                "api_method": "detailscreen"
            }
        )

    def test_extract_gridscreen_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "vod-service",
                        "header": {
                            "x-original-uri": "/vod-service/v2/gridscreen/omw_hzn4_vod/crid:~~2F~~2Fschange.com~~2F99cd6cce-f330-4dda-a8cb-190711ffb735?country=de&language=en&sortType=ordinal&sortDirection=as"
                        }
                    })
            },
            {
                "app": "vod-service",
                "header_x-original-uri": "/vod-service/v2/gridscreen/omw_hzn4_vod/crid:~~2F~~2Fschange.com~~2F99cd6cce-f330-4dda-a8cb-190711ffb735?country=de&language=en&sortType=ordinal&sortDirection=as",
                "api_method": "gridscreen"
            }
        )

    def test_extract_learn_actions_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "discovery-service",
                        "header": {
                            "x-original-uri": "/discovery-service/v1/learn-actions"
                        }
                    })
            },
            {
                "app": "discovery-service",
                "header_x-original-uri": "/discovery-service/v1/learn-actions",
                "api_method": "learn-actions"
            }
        )

    def test_extract_search_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "discovery-service",
                        "header": {
                            "x-original-uri": "/discovery-service/v1/search/contents?clientType=399&contentSourceId=1&searchTerm=News&startResults=0&maxResults=1&includeNotEntitled=true&profileId=d53facd0-cab"
                        }
                    })
            },
            {
                "app": "discovery-service",
                "header_x-original-uri": "/discovery-service/v1/search/contents?clientType=399&contentSourceId=1&searchTerm=News&startResults=0&maxResults=1&includeNotEntitled=true&profileId=d53facd0-cab",
                "api_method": "search"
            }
        )

    def test_extract_recommendations_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "discovery-service",
                        "header": {
                            "x-original-uri": "/discovery-service/v1/recommendations/more-like-this?clientType=305&contentSourceId=1&contentSourceId=2&profileId=12c41366-0a93-4e54-b1b6-efcf22c132b9_be~~23Mast"
                        }
                    })
            },
            {
                "app": "discovery-service",
                "header_x-original-uri": "/discovery-service/v1/recommendations/more-like-this?clientType=305&contentSourceId=1&contentSourceId=2&profileId=12c41366-0a93-4e54-b1b6-efcf22c132b9_be~~23Mast",
                "api_method": "recommendations"
            }
        )

    def test_extract_channels_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "session-service",
                        "header": {
                            "x-original-uri": "/session-service/session/channels/Nederland_1_HD?startTime=2018-01-16T15:07:39Z"
                        }
                    })
            },
            {
                "app": "session-service",
                "header_x-original-uri": "/session-service/session/channels/Nederland_1_HD?startTime=2018-01-16T15:07:39Z",
                "api_method": "channels"
            }
        )

    def test_extract_cpes_api_method(self):
        self.assert_parsing(
            {
                "message": json.dumps(
                    {
                        "app": "session-service",
                        "header": {
                            "x-original-uri": "/session-service/session/cpes/3C36E4-EOSSTB-003356297204/replay/events/crid:~~2F~~2Fbds.tv~~2F19867244,imi:00100000000E8423    => extract cpes"
                        }
                    })
            },
            {
                "app": "session-service",
                "header_x-original-uri": "/session-service/session/cpes/3C36E4-EOSSTB-003356297204/replay/events/crid:~~2F~~2Fbds.tv~~2F19867244,imi:00100000000E8423    => extract cpes",
                "api_method": "cpes"
            }
        )
