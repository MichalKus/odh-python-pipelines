from datetime import datetime

from applications.log_parsing.nokia_vrm.driver import create_event_creators
from common.log_parsing.metadata import ParsingException
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration
from common.log_parsing.timezone_metadata import timezones


class NokiaVrmMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_scheduler_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-scheduler-BS/logs/scheduler_bs_audit.log",
                "message": "26-Feb-2018 17:41:06.356 | INFO  |  | a238c94a-0a16-46f8-ba53-c207444bd8d2 | INTERNAL | 172.23.41.73 | [[GET] /scheduler/web/Record/addByProgram] | [{schema=1.0, eventId=crid:~~2F~~2Ftelenet.be~~2F7eda8354-982e-4951-ba32-7aa1219d7004,imi:00100000001C566C, userId=subscriber_000556_be}] | Entity not found (info) | Not found Event entity with programId field equals to crid:~~2F~~2Ftelenet.be~~2F7eda8354-982e-4951-ba32-7aa1219d7004,imi:00100000001C566C"
            },
            {
                "@timestamp": datetime(2018, 2, 26, 17, 41, 6, 356000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "field1": "",
                "level": "INFO",
                "event_id": "a238c94a-0a16-46f8-ba53-c207444bd8d2",
                "domain": "INTERNAL",
                "ip": "172.23.41.73",
                "method": "[[GET] /scheduler/web/Record/addByProgram]",
                "params": "[{schema=1.0, eventId=crid:~~2F~~2Ftelenet.be~~2F7eda8354-982e-4951-ba32-7aa1219d7004,imi:00100000001C566C, userId=subscriber_000556_be}]",
                "description": "Entity not found (info)",
                "message": "Not found Event entity with programId field equals to crid:~~2F~~2Ftelenet.be~~2F7eda8354-982e-4951-ba32-7aa1219d7004,imi:00100000001C566C"
            }
        )

    def test_console_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-dvr-console-BS/logs/console_bs_audit.log",
                "message": "20-Sep-2017 15:53:00.010 | ERROR | 41cd2f05-5f44-4cc9-97cc-5cb9007b27bf | INTERNAL |  | [[GET] /dvr-console/data/sysadmin/status] | [{entriesPageSize=1000, sortDirection=asc, entriesStartIndex=0, sortField=instanceName, count=true}] | Request to service ended with communication error | NoRouteToHostException invoking request GET http://10.95.97.91:8088/auth. Exception message: No route to host (Host unreachable)"
            },
            {
                "@timestamp": datetime(2017, 9, 20, 15, 53, 0, 10000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "ip": "",
                "event_id": "41cd2f05-5f44-4cc9-97cc-5cb9007b27bf",
                "domain": "INTERNAL",
                "method": "[[GET] /dvr-console/data/sysadmin/status]",
                "params": "[{entriesPageSize=1000, sortDirection=asc, entriesStartIndex=0, sortField=instanceName, count=true}]",
                "description": "Request to service ended with communication error",
                "message": "NoRouteToHostException invoking request GET http://10.95.97.91:8088/auth. Exception message: No route to host (Host unreachable)"
            }
        )

    def test_authentication_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-dvr-console-BS/logs/authentication_bs_audit.log",
                "message": "27-Nov-2017 11:11:14.331 | WARN  | 1b5e781d-3c79-4ecc-b369-ee2bd25b6fe9 | CONTROL | 10.95.97.61 | [[GET] /auth/control/checkAlive] | [{depth=deep}] | Service checkAlive request with response false | user DS is not alive"
            },
            {
                "@timestamp": datetime(2017, 11, 27, 11, 11, 14, 331000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "WARN",
                "event_id": "1b5e781d-3c79-4ecc-b369-ee2bd25b6fe9",
                "domain": "CONTROL",
                "ip": "10.95.97.61",
                "method": "[[GET] /auth/control/checkAlive]",
                "params": "[{depth=deep}]",
                "description": "Service checkAlive request with response false",
                "message": "user DS is not alive"
            }
        )

    def test_cdvr_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-dvr-console-BS/logs/cdvr_bs_audit.log",
                "message": "17-Jan-2018 11:30:30.035 | ERROR | f37d72f0-d11d-4adf-9144-02db7d109397 | INTERNAL | local | [Job execute] | [{}] | Request to service returns unexpected HTTP response status code | Unexpected HTTP response status code 404 invoking request GET http://10.16.174.129:5929"
            },
            {
                "@timestamp": datetime(2018, 1, 17, 11, 30, 30, 35000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "event_id": "f37d72f0-d11d-4adf-9144-02db7d109397",
                "domain": "INTERNAL",
                "ip": "local",
                "method": "[Job execute]",
                "params": "[{}]",
                "description": "Request to service returns unexpected HTTP response status code",
                "message": "Unexpected HTTP response status code 404 invoking request GET http://10.16.174.129:5929"
            }
        )

    def test_epg_ds_audit(self):
        self.assert_parsing({
            "topic": "nokiavrmds_epgaudit",
            "source": "/opt/vrm/jetty-dvr-console-BS/logs/epg_audit.log",
            "message": "29-Nov-2017 19:30:01.480 | ERROR | 685767e5-7da7-4b15-bc90-c2dae3d129e6 | 640572db-5331-48c3-a446-072ad80c02d1 | INTERNAL | 10.95.97.66 | [[PUT] /epg/data/Event] | [{schema=2.0, entries=true, query=.where(eq(e.internalId,49229),or(eq(e.locked,null()),eq(e.locked,false()))), count=true}] | Data access unrecoverable error | Unrecoverable error details: Duplicate entry '' for key 'alternateId'"
        },
            {
                "@timestamp": datetime(2017, 11, 29, 19, 30, 1, 480000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "event_id_1": "685767e5-7da7-4b15-bc90-c2dae3d129e6",
                "event_id_2": "640572db-5331-48c3-a446-072ad80c02d1",
                "domain": "INTERNAL",
                "ip": "10.95.97.66",
                "method": "[[PUT] /epg/data/Event]",
                "params": "[{schema=2.0, entries=true, query=.where(eq(e.internalId,49229),or(eq(e.locked,null()),eq(e.locked,false()))), count=true}]",
                "description": "Data access unrecoverable error",
                "message": "Unrecoverable error details: Duplicate entry '' for key 'alternateId'"
            })

    def test_epg_bs_audit(self):
        self.assert_parsing({
            "topic": "nokiavrmbs_epgaudit",
            "source": "/opt/vrm/jetty-dvr-console-BS/logs/epg_audit.log",
            "message": "29-Nov-2017 19:30:01.480 | ERROR | 685767e5-7da7-4b15-bc90-c2dae3d129e6 | 640572db-5331-48c3-a446-072ad80c02d1 | INTERNAL | 10.95.97.66 | [[PUT] /epg/data/Event] | [{schema=2.0, entries=true, query=.where(eq(e.internalId,49229),or(eq(e.locked,null()),eq(e.locked,false()))), count=true}] | Data access unrecoverable error | Unrecoverable error details: Duplicate entry '' for key 'alternateId'"
        },
            {
                "@timestamp": datetime(2017, 11, 29, 19, 30, 1, 480000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "event_id_1": "685767e5-7da7-4b15-bc90-c2dae3d129e6",
                "event_id_2": "640572db-5331-48c3-a446-072ad80c02d1",
                "domain": "INTERNAL",
                "ip": "10.95.97.66",
                "method": "[[PUT] /epg/data/Event]",
                "params": "[{schema=2.0, entries=true, query=.where(eq(e.internalId,49229),or(eq(e.locked,null()),eq(e.locked,false()))), count=true}]",
                "description": "Data access unrecoverable error",
                "message": "Unrecoverable error details: Duplicate entry '' for key 'alternateId'"
            })

    def test_epg_audit_fails_with_non_existing_topic(self):
        row = {
            "topic": "invalid_topic",
            "source": "/opt/vrm/jetty-dvr-console-BS/logs/epg_audit.log",
            "message": "29-Nov-2017 19:30:01.480 | ERROR | 685767e5-7da7-4b15-bc90-c2dae3d129e6 | 640572db-5331-48c3-a446-072ad80c02d1 | INTERNAL | 10.95.97.66 | [[PUT] /epg/data/Event] | [{schema=2.0, entries=true, query=.where(eq(e.internalId,49229),or(eq(e.locked,null()),eq(e.locked,false()))), count=true}] | Data access unrecoverable error | Unrecoverable error details: Duplicate entry '' for key 'alternateId'"
        }
        with self.assertRaises(ParsingException):
            self.event_creators.get_parsing_context(row).event_creator.create(row)

    def test_cdvr_audit(self):
        self.assert_parsing({
            "source": "/opt/vrm/jetty-dvr-console-DS/logs/cDVR_audit.log",
            "message": "27-Nov-2017 03:30:24.653 | ERROR | 62f6bf96-e92c-4eea-bea0-4571e4dbc94a | dd69dbe2-0e53-4712-85ce-1f395a7c0732 | INTERNAL |  | [[GET] /cDVR/data/Record] | [{schema=1.0, byUserId=Jef_be, fields=eventId,actualStartTime,year,episode,seasonNumber,source,seriesId,alreadyWatched,duration,seasonName,name,actualEndTime,pinProtected,startTime,id,endTime,programId,channelId,status}] | Data access unrecoverable error | Unrecoverable error details: Got error 4009 'Cluster Failure' from NDBCLUSTER. Exception message: java.sql.SQLException: Got error 4009 'Cluster Failure' from NDBCLUSTER"
        },
            {
                "@timestamp": datetime(2017, 11, 27, 3, 30, 24, 653000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "event_id_1": "62f6bf96-e92c-4eea-bea0-4571e4dbc94a",
                "event_id_2": "dd69dbe2-0e53-4712-85ce-1f395a7c0732",
                "domain": "INTERNAL",
                "ip": "",
                "method": "[[GET] /cDVR/data/Record]",
                "params": "[{schema=1.0, byUserId=Jef_be, fields=eventId,actualStartTime,year,episode,seasonNumber,source,seriesId,alreadyWatched,duration,seasonName,name,actualEndTime,pinProtected,startTime,id,endTime,programId,channelId,status}]",
                "description": "Data access unrecoverable error",
                "message": "Unrecoverable error details: Got error 4009 'Cluster Failure' from NDBCLUSTER. Exception message: java.sql.SQLException: Got error 4009 'Cluster Failure' from NDBCLUSTER"
            })

    def test_user_audit(self):
        self.assert_parsing({
            "source": "/opt/vrm/jetty-dvr-console-DS/logs/user_audit.log",
            "message": "27-Nov-2017 10:35:01.718 | ERROR | d493b5da-13a4-4504-81af-484bc09329b7 | b381adc9-9d87-4dbf-8c26-b58bc92c24e9 | INTERNAL | 10.95.97.65 | [[GET] /user/data/User] | [{schema=1.0, byId=dfb5e30d-c47e-4f10-94ea-e8faba89c345_be, fields=occupied,quota,quotaType,occupiedNumRecordings,occupiedBytes,disabled}] | Data access unrecoverable error | Unrecoverable error details: Connection refused (Connection refused)"
        },
            {
                "@timestamp": datetime(2017, 11, 27, 10, 35, 1, 718000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "event_id_1": "d493b5da-13a4-4504-81af-484bc09329b7",
                "event_id_2": "b381adc9-9d87-4dbf-8c26-b58bc92c24e9",
                "domain": "INTERNAL",
                "ip": "10.95.97.65",
                "method": "[[GET] /user/data/User]",
                "params": "[{schema=1.0, byId=dfb5e30d-c47e-4f10-94ea-e8faba89c345_be, fields=occupied,quota,quotaType,occupiedNumRecordings,occupiedBytes,disabled}]",
                "description": "Data access unrecoverable error",
                "message": "Unrecoverable error details: Connection refused (Connection refused)"
            })

    def test_lgienh_api_audit(self):
        self.assert_parsing({
            "source": "/opt/vrm/jetty-dvr-console-DS/logs/lgienhapi_bs.log",
            "message": "2018-01-17 12:50:54.336 | INFO  | RecordingsEndpoint | getRecordings: user: 17f5a9c1-db47-4f1d-939a-842a72f57b59_be"
        },
            {
                "@timestamp": datetime(2018, 1, 17, 12, 50, 54, 336000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "endpoint": "RecordingsEndpoint",
                "request": "getRecordings: user: 17f5a9c1-db47-4f1d-939a-842a72f57b59_be"
            })

    def test_schange_import_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-dvr-console-BS/logs/schange_import_bs_audit.log",
                "message": "17-Jan-2018 13:10:01.427 | INFO  | 1563377d-8331-4ac1-93c3-089267cb815f | INTERNAL | local | [Job execute] | [{}] | Request to service returns a operation status response | Error response: title = Data Access Error, description = Data access unrecoverable error, result code = 5003"
            },
            {
                "@timestamp": datetime(2018, 1, 17, 13, 10, 01, 427000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "event_id": "1563377d-8331-4ac1-93c3-089267cb815f",
                "domain": "INTERNAL",
                "ip": "local",
                "method": "[Job execute]",
                "params": "[{}]",
                "description": "Request to service returns a operation status response",
                "message": "Error response: title = Data Access Error, description = Data access unrecoverable error, result code = 5003"
            }
        )

    def vspp_adapter_bs_audit(self):
        self.assert_parsing(
            {
                "source": "/opt/vrm/jetty-dvr-console-BS/logs/schange_import_bs_audit.log",
                "message": "17-Jan-2018 13:01:42.327 | INFO  | c1436d45-7d9b-4166-a84f-c2985a5e34a7 | API | 10.16.174.129 | [[POST] /vspp-adapter/Notification/Notify] | [{}] | Request execution finished | Request finished successfully"
            },
            {
                "@timestamp": datetime(2018, 1, 17, 13, 1, 42, 327000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "event_id": "c1436d45-7d9b-4166-a84f-c2985a5e34a7",
                "domain": "API",
                "ip": "10.16.174.129",
                "method": "[[POST] /vspp-adapter/Notification/Notify]",
                "params": "[{}]",
                "description": "Request execution finished",
                "message": "Request finished successfully"
            }
        )
