from datetime import datetime

from applications.log_parsing.stagis.driver import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class StagisMessageParsingTestCase(BaseMultipleMessageParsingTestCase):

    event_creators = create_event_creators()

    def test_general(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_gen_v1",
                "message": "2017-06-03 03:45:27,624 | INFO  | Catalog Ingester   | 7ce7119-4504-4908-8041-0fb10cbe26b6 | 369                                 | EntityCounter                       | [Model] Model state after committing transaction [Sequence number: 14918942705019671 - Timestamp: 4/11/2017 7:04:30 AM - Number: 23676617] Entities: 401661 - Links: 535328 - Channels: 271 - Events: 201876 - Programs: 178085 - Groups: 21429"
            },
            {
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000),
                "level": "INFO",
                "instance_name": "Catalog Ingester",
                "causality_id": "7ce7119-4504-4908-8041-0fb10cbe26b6",
                "thread_id": "369",
                "class_name": "EntityCounter",
                "message": "[Model] Model state after committing transaction [Sequence number: 14918942705019671 - Timestamp: 4/11/2017 7:04:30 AM - Number: 23676617] Entities: 401661 - Links: 535328 - Channels: 271 - Events: 201876 - Programs: 178085 - Groups: 21429"
            }
        )

    def test_error(self):
        self.assert_parsing(
            {
                "topic": "vagrant_in_eosdtv_lab5aobo_tst_heapp_stagis_log_err_v1",
                "message": "2017-10-30 10:01:05,169 | ERROR | Stagis | cfdd16c-ca17-4ab7-a2f6-5b4b4b966d77 | DefaultQuartzScheduler_Worker-1 | DataWorkflowCore | Starting the Core Provider failed."
            },
            {
                "@timestamp": datetime(2017, 10, 30, 10, 1, 5, 169000),
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
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000),
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
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000),
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
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000),
                "message": "LogParameterInspector - ------------------------------------------------------LogParameterInspector - Incoming call (aa84fcd0-0a23-4aa0-b66e-366559148853): net.pipe://localhost/STAGIS_EE_Services/ChannelService/GetDisplayNameLogParameterInspector - Arguments:LogParameterInspector -     List, Items: 1, First Item: Language: NL, Kind: NULL, Name: NPO 1LogParameterInspector -     ned1LogParameterInspector -     True\", \"hostname\": \"test1\", \"reason\": \"Fields amount not equal values amount\"}"
            }
        )
