from dateutil.tz import tzoffset

from applications.log_parsing.think_analytics.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from datetime import datetime


class ThinkAnalyticsMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_httpaccess(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_thinkanalytics_httpaccess_log_v1",
                "source": "localhost_access_log",
                "message": "[19/Jun/2017:02:06:17 +0200] 172.30.189.59 http-0.0.0.0-8080-64 GET /RE/REController.do?contentSourceId=1&clientType=399&method=lgiAdaptiveSearch&subscriberId=278e3270-4d12-11e7-85f5-e5a72ae6734d_nl HTTP/1.1 200 14"
            },
            {
                "@timestamp": datetime(2017, 6, 19, 02, 06, 17).replace(tzinfo=tzoffset(None, 7200)),
                "ip": "172.30.189.59",
                "thread": "http-0.0.0.0-8080-64",
                "http_method": "GET",
                "http_version": "HTTP/1.1",
                "response_code": "200",
                "response_time": "14",
                "contentSourceId": "1",
                "clientType": "399",
                "method": "lgiAdaptiveSearch",
                "subscriberId": "278e3270-4d12-11e7-85f5-e5a72ae6734d_nl",
                "action": "/RE/REController.do"
            }
        )

    def test_resystemout(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_thinkanalytics_resystemout_log_v1",
                "source": "RE_SystemOut.log",
                "message": "[29/09/17 13:00:23.944 CEST] WARN  - RecommendationServiceController.handleRequestInternal(211) : [DAWN_0107] - Failed to get customer data from Traxis for profile Id: Jef_be_be~~23MasterProfile: [DAWN_0103] - Error calling Traxis.Web  for Jef_be_be~~23MasterProfile: HTTP Code: 400, HTTP Message: Bad Request, Traxis Message: Invalid parameter 'ProfileId', value 'Jef_be_be#MasterProfile'"
            },
            {
                "@timestamp": datetime(2017, 9, 29, 13, 00, 23, 944000),
                "level": "WARN",
                "script": "RecommendationServiceController.handleRequestInternal(211)",
                "message": "[DAWN_0107] - Failed to get customer data from Traxis for profile Id: Jef_be_be~~23MasterProfile: [DAWN_0103] - Error calling Traxis.Web  for Jef_be_be~~23MasterProfile: HTTP Code: 400, HTTP Message: Bad Request, Traxis Message: Invalid parameter 'ProfileId', value 'Jef_be_be#MasterProfile'"
            }
        )

    def test_remonsystemout(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_thinkanalytics_remonsystemout_log_v1",
                "source": "REMON_SystemOut.log",
                "message": "[29/09/17 01:15:00.141 CEST] WARN  - LGITopListManager.validateTopLists(113) : [NO_ENTRIES_FOR_EXPECTED_TOP_LIST] - Expected Top List MostPurchased^TVOD_Currents is missing or has no entries."
            },
            {
                "@timestamp": datetime(2017, 9, 29, 01, 15, 00, 141000),
                "level": "WARN",
                "script": "LGITopListManager.validateTopLists(113)",
                "type": "NO_ENTRIES_FOR_EXPECTED_TOP_LIST",
                "message": "Expected Top List MostPurchased^TVOD_Currents is missing or has no entries."
            }
        )

    def test_central(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_thinkanalytics_central_log_v1",
                "source": "Central.log",
                "message": '''"Thu 05/10/17","02:50:01","","Event Log Stopped","be-l-p-obo00336","","","","Customer"'''
            },
            {
                "@timestamp": datetime(2017, 10, 05, 02, 50, 01),
                "level": "",
                "message": "Event Log Stopped",
                "thread": "be-l-p-obo00336",
                "c0": "",
                "c1": "",
                "c2": "",
                "role": "Customer"
            }
        )

    def test_thinkenterprise(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_thinkenterprise_central_log_v1",
                "source": "thinkenterprise.log",
                "message": "2017-09-29 02:50:44,608: INFO - ThinkEnterprise: rmi://be-l-p-obo00335:55969"
            },
            {
                "@timestamp": datetime(2017, 9, 29, 02, 50, 44, 608000),
                "level": "INFO",
                "message": "ThinkEnterprise: rmi://be-l-p-obo00335:55969"
            }
        )

    def test_gcollector(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_gcollector_central_log_v1",
                "source": "gcollector.log",
                "message": "2017-09-29T07:03:38.835+0200: 908973.815: [GC [1 CMS-initial-mark: 997339K(1398144K)] 1032156K(2027264K), 0.0337620 secs] [Times: user=0.03 sys=0.00, real=0.04 secs]"
            },
            {
                "@timestamp": datetime(2017, 9, 29, 07, 03, 38, 835000).replace(tzinfo=tzoffset(None, 7200)),
                "process_uptime": "908973.815",
                "message": "[GC [1 CMS-initial-mark: 997339K(1398144K)] 1032156K(2027264K), 0.0337620 secs] [Times: user=0.03 sys=0.00, real=0.04 secs]"
            }
        )

    def test_server(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_gcollector_server_log_v1",
                "source": "server.log",
                "message": "2017-10-05 15:07:27,281 WARN  [com.mchange.v2.resourcepool.BasicResourcePool] (C3P0PooledConnectionPoolManager[identityToken->2vlxe59qgs2ym41at6ny1|2efd4b56, dataSourceName->creRepStatus]-HelperThread-#5) com.mchange.v2.resourcepool.BasicResourcePool$ScatteredAcquireTask@90a251f -- Acquisition Attempt Failed!!! Clearing pending acquires. While trying to acquire a needed new resource, we failed to succeed more than the maximum number of allowed acquisition attempts (30). Last acquisition attempt exception: : java.sql.SQLException: ORA-01017: invalid username/password; logon denied"
            },
            {
                "@timestamp": datetime(2017, 10, 5, 15, 07, 27, 281000),
                "level": "WARN",
                "class_name": "com.mchange.v2.resourcepool.BasicResourcePool",
                "thread": "C3P0PooledConnectionPoolManager[identityToken->2vlxe59qgs2ym41at6ny1|2efd4b56, dataSourceName->creRepStatus]-HelperThread-#5",
                "message": "com.mchange.v2.resourcepool.BasicResourcePool$ScatteredAcquireTask@90a251f -- Acquisition Attempt Failed!!! Clearing pending acquires. While trying to acquire a needed new resource, we failed to succeed more than the maximum number of allowed acquisition attempts (30). Last acquisition attempt exception: : java.sql.SQLException: ORA-01017: invalid username/password; logon denied"

            }
        )

    def test_reingest(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_gcollector_reingest_log_v1",
                "source": "RE_Ingest.log",
                "message": """-- Start of RE_SocialModel.log --
Started ./runBuildSocialModel.sh  Mon Aug 28 15:45:09 CEST 2017
Started /apps/ThinkAnalytics/ModelAnalysis/bin/buildModelAndRefreshMemory.sh  Mon Aug 28 15:45:09 CEST 2017
Started /apps/ThinkAnalytics/ModelAnalysis/bin/buildSocialModel.sh  Mon Aug 28 15:45:09 CEST 2017
Buildfile: install-run.xml

check_kwiz_libs:

runVODSubscriberPrepareBuildData:
[Java[RunPlanTask]] java.lang.Exception: Failed to run plan:VODSubscriberPrepareBuildData
[Java[RunPlanTask]] 	at com.thinkanalytics.re.ant.RunPlan.execute(RunPlan.java:279)
[Java[RunPlanTask]] 	at com.thinkanalytics.re.ant.RunPlan.main(RunPlan.java:370)
[Java[RunPlanTask]] 	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
[Java[RunPlanTask]] 	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
[Java[RunPlanTask]] 	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
[Java[RunPlanTask]] 	at java.lang.reflect.Method.invoke(Method.java:606)
[Java[RunPlanTask]] 	at com.kwiz.kd.util.KwizLoader.main(Unknown Source)

BUILD FAILED
/app/apps/ThinkAnalytics/ModelAnalysis/setup/install-run.xml:20: Failed to run plan (VODSubscriberPrepareBuildData) reason:Java returned: 1

Total time: 12 seconds
[28/08/2017-15:45:23-CEST] ERROR - FAILED to run social model building.
[28/08/2017-15:45:23-CEST] ERROR - FAILED to rebuild the social model
Finished ./runBuildSocialModel.sh  Mon Aug 28 15:45:23 CEST 2017"""
            },
            {
                "@timestamp": datetime(2017, 8, 28, 15, 45, 9),
                "started_script": "./runBuildSocialModel.sh",
                "finished_script": "./runBuildSocialModel.sh",
                "finished_time": datetime(2017, 8, 28, 15, 45, 23),
                "duration": 14,
                "message": "\nStarted /apps/ThinkAnalytics/ModelAnalysis/bin/buildModelAndRefreshMemory.sh  Mon Aug 28 15:45:09 CEST 2017\nStarted /apps/ThinkAnalytics/ModelAnalysis/bin/buildSocialModel.sh  Mon Aug 28 15:45:09 CEST 2017\nBuildfile: install-run.xml\n\ncheck_kwiz_libs:\n\nrunVODSubscriberPrepareBuildData:\n[Java[RunPlanTask]] java.lang.Exception: Failed to run plan:VODSubscriberPrepareBuildData\n[Java[RunPlanTask]] \tat com.thinkanalytics.re.ant.RunPlan.execute(RunPlan.java:279)\n[Java[RunPlanTask]] \tat com.thinkanalytics.re.ant.RunPlan.main(RunPlan.java:370)\n[Java[RunPlanTask]] \tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n[Java[RunPlanTask]] \tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)\n[Java[RunPlanTask]] \tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n[Java[RunPlanTask]] \tat java.lang.reflect.Method.invoke(Method.java:606)\n[Java[RunPlanTask]] \tat com.kwiz.kd.util.KwizLoader.main(Unknown Source)\n\nBUILD FAILED\n/app/apps/ThinkAnalytics/ModelAnalysis/setup/install-run.xml:20: Failed to run plan (VODSubscriberPrepareBuildData) reason:Java returned: 1\n\nTotal time: 12 seconds\n[28/08/2017-15:45:23-CEST] ERROR - FAILED to run social model building.\n[28/08/2017-15:45:23-CEST] ERROR - FAILED to rebuild the social model\n"
            }
        )
