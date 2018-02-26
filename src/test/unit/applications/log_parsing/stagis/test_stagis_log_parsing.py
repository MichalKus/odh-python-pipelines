from datetime import datetime

from applications.log_parsing.stagis.driver import create_event_creators
from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration


class StagisMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_general(self):
        test_cases = [
            ({
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | TVA Delta Server respond with status code 'OK' in '1576' ms"

            }, {
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "instance_name": "Catalog Ingester",
                "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                "thread_id": "369",
                "class_name": "EntityCounter",
                "task": "TVA Delta Server response",
                "duration": 1576,
                "status": "OK",
                "message": "TVA Delta Server respond with status code 'OK' in '1576' ms"
            }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | TVA Delta Request Starting => http://BE-W-P-OBO00159:9000/TVAMain?SyncAfter=15132539107630000, sequence number: 15132539107630000"

             }, {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "task": "TVA Delta Server request",
                 "sequence_number": "15132539107630000",
                 "message": "TVA Delta Request Starting => http://BE-W-P-OBO00159:9000/TVAMain?SyncAfter=15132539107630000, sequence number: 15132539107630000"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | Received Delta Server Notification Sequence Number: 15132539107630000, Last Sequence Number: 15132539107630000, LogicalServerId: 'PRODIS_170906160628'"
             }, {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "task": "Notification",
                 "sequence_number": "15132539107630000",
                 "message": "Received Delta Server Notification Sequence Number: 15132539107630000, Last Sequence Number: 15132539107630000, LogicalServerId: 'PRODIS_170906160628'"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | [Model] Model state after committing transaction [Sequence number: 15132539721087218 - Timestamp: 14/12/2017 12:19:32 - Number: 63861022] Entities: 447464 - Links: 1394780 - Channels: 350 - Events: 242420 - Programs: 132061 - Groups: 59377 - OnDemandPrograms: 7993 - BroadcastEvents: 5263"

             },
             {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "task": "Committing Transaction",
                 "sequence_number": "15132539721087218",
                 "number": "63861022",
                 "entities": 447464,
                 "links": 1394780,
                 "channels": 350,
                 "events": 242420,
                 "programs": 132061,
                 "groups": 59377,
                 "on_demand_programs": 7993,
                 "broadcast_events": 5263,
                 "message": "[Model] Model state after committing transaction [Sequence number: 15132539721087218 - Timestamp: 14/12/2017 12:19:32 - Number: 63861022] Entities: 447464 - Links: 1394780 - Channels: 350 - Events: 242420 - Programs: 132061 - Groups: 59377 - OnDemandPrograms: 7993 - BroadcastEvents: 5263"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | Just some message, no substring match"

             },
             {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "message": "Just some message, no substring match"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | TVA Delta Server respond - substring match, but wrong regex"

             },
             {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "message": "TVA Delta Server respond - substring match, but wrong regex"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | TVA Delta Request Starting - substring match, but wrong regex"

             },
             {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "message": "TVA Delta Request Starting - substring match, but wrong regex"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | Received Delta Server Notification - substring match, but wrong regex"

             },
             {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "message": "Received Delta Server Notification - substring match, but wrong regex"
             }),
            ({
                 "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                 "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | Model state after committing transaction - substring match, but wrong regex"

             },
             {
                 "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                 "level": "INFO",
                 "instance_name": "Catalog Ingester",
                 "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                 "thread_id": "369",
                 "class_name": "EntityCounter",
                 "message": "Model state after committing transaction - substring match, but wrong regex"
             })
        ]

        for test_message, parsed_message in test_cases:
            self.assert_parsing(test_message, parsed_message)

    def test_error(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_err_v1",
                "message": "2017-10-30 10:01:05,169 | ERROR | Stagis | cfdd16c-ca17-4ab7-a2f6-5b4b4b966d77 | DefaultQuartzScheduler_Worker-1 | DataWorkflowCore | Starting the Core Provider failed."
            },
            {
                "@timestamp": datetime(2017, 10, 30, 10, 1, 5, 169000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "ERROR",
                "instance_name": "Stagis",
                "causality_id": "cfdd16c-ca17-4ab7-a2f6-5b4b4b966d77",
                "thread_id": "DefaultQuartzScheduler_Worker-1",
                "class_name": "DataWorkflowCore",
                "message": "Starting the Core Provider failed."
            }
        )


    def test_interface(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_interface_log_gen_v1",
                "message": "2017-06-03 03:45:27,624                    | LogParameterInspector                        | 369                           | EntityCounter                           | Enrich core transaction with Productizer enricher started. "
            },
            {
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "instance_name": "LogParameterInspector",
                "thread_id": "369",
                "class_name": "EntityCounter",
                "message": "Enrich core transaction with Productizer enricher started."
            }
        )

    def test_corecommit(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_corecommit_log_err_v1",
                "message": "2017-06-03 03:45:27,624  | INFO | some_instance_name                        | 5465                         | 369                           | EntityCounter                           | Enrich core transaction with Productizer enricher started. "
            },
            {
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "level": "INFO",
                "instance_name": "some_instance_name",
                "causality_id": "5465",
                "thread_id": "369",
                "class_name": "EntityCounter",
                "message": "Enrich core transaction with Productizer enricher started."
            }
        )

    def test_wcf(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_wcf_log_gen_v1",
                "message": "2017-06-03 03:45:27,624 | LogParameterInspector - ------------------------------------------------------\n2017-07-24 14:49:51,721 | LogParameterInspector - Incoming call (aa84fcd0-0a23-4aa0-b66e-366559148853): net.pipe://localhost/STAGIS_EE_Services/ChannelService/GetDisplayName\n2017-07-24 14:49:51,721 | LogParameterInspector - Arguments: \n2017-07-24 14:49:51,721 | LogParameterInspector -     List, Items: 1, First Item: Language: NL, Kind: NULL, Name: NPO 1\n2017-07-24 14:49:51,722 | LogParameterInspector -     ned1\n2017-07-24 14:49:51,722 | LogParameterInspector -     True\", \"hostname\": \"test1\", \"reason\": \"Fields amount not equal values amount\"}"
            },
            {
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000).replace(tzinfo=timezones["Europe/Amsterdam"]),
                "message": "LogParameterInspector - ------------------------------------------------------LogParameterInspector - Incoming call (aa84fcd0-0a23-4aa0-b66e-366559148853): net.pipe://localhost/STAGIS_EE_Services/ChannelService/GetDisplayNameLogParameterInspector - Arguments:LogParameterInspector -     List, Items: 1, First Item: Language: NL, Kind: NULL, Name: NPO 1LogParameterInspector -     ned1LogParameterInspector -     True\", \"hostname\": \"test1\", \"reason\": \"Fields amount not equal values amount\"}"
            }
        )
