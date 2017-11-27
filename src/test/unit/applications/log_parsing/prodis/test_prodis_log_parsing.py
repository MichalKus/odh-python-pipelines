from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from applications.log_parsing.prodis.driver import create_event_creators
from datetime import datetime


class ProdisMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_prodis_ws_5(self):
        self.assert_parsing(
            {
                "source": "PRODIS_WS.log",
                "message": "2017-09-28 13:39:11,238 | DEBUG | Asset Propagation Thread | TSTV 3D Port | [Asset: 9a7b25dd-d5e7-4d9b-b91d-777002e11008] - Asset does not need repropagation to videoserver according to the adapter specific data"
            },
            {
                "@timestamp": datetime(2017, 9, 28, 13, 39, 11, 238000),
                "level": "DEBUG",
                "thread_name": "Asset Propagation Thread",
                "instance_name": "TSTV 3D Port",
                "message": "[Asset: 9a7b25dd-d5e7-4d9b-b91d-777002e11008] - Asset does not need repropagation to videoserver according to the adapter specific data"
            }
        )

    def test_prodis_ws_error_5(self):
        self.assert_parsing(
            {
                "source": "PRODIS_WS.Error.log",
                "message": "2017-10-04 14:02:30,482 | ERROR | Asset Propagation Thread                           | TSTV 3D Port                                       | [Asset: 84917e83-e618-4afa-aa91-4d5c374514c2] - Asset has failed with message 'Received unexpected reply during CreateOrUpdateContent, HttpStatusCode : NotFound'."
            },
            {
                "@timestamp": datetime(2017, 10, 4, 14, 2, 30, 482000),
                "level": "ERROR",
                "thread_name": "Asset Propagation Thread",
                "instance_name": "TSTV 3D Port",
                "message": "[Asset: 84917e83-e618-4afa-aa91-4d5c374514c2] - Asset has failed with message 'Received unexpected reply during CreateOrUpdateContent, HttpStatusCode : NotFound'."
            }
        )

    def test_prodis_ws_6(self):
        self.assert_parsing(
            {
                "source": "PRODIS_WS.log",
                "message": "2017-06-03 03:45:27,624 | INFO  | Asset Propagation Thread                           | TSTV 3D Port                                       | TSTV Recording Scheduled | P=P;ChId=0062;ChName=NPO 1;AssetId=92148691-1cb3-44fd-a313-809ef36b0604;Title=Boeken;ResponseCode=RecordingInstructionSucceeded;Message='CreateOrUpdate' message for asset '92148691-1cb3-44fd-a313-809ef36b0604' has been sent to content server."
            },
            {
                "@timestamp": datetime(2017, 6, 3, 3, 45, 27, 624000),
                "level": "INFO",
                "thread_name": "Asset Propagation Thread",
                "instance_name": "TSTV 3D Port",
                "component": "TSTV Recording Scheduled",
                "message": "P=P;ChId=0062;ChName=NPO 1;AssetId=92148691-1cb3-44fd-a313-809ef36b0604;Title=Boeken;ResponseCode=RecordingInstructionSucceeded;Message='CreateOrUpdate' message for asset '92148691-1cb3-44fd-a313-809ef36b0604' has been sent to content server."
            }
        )

    def test_prodis_general(self):
        self.assert_parsing(
            {
                "source": "PRODIS.log",
                "message": "2017-10-03 16:26:06,782  INFO [1] - Registered PRODIS client '21' with information: be-w-p-obo00159, 169.254.207.71, 00:50:56:B2:40:6B, a_jlambregts, admin, 8040, 2.3 2017-July-05 #1 Release patch 2"
            },
            {
                "@timestamp": datetime(2017, 10, 3, 16, 26, 6, 782000),
                "level": "INFO",
                "thread": "1",
                "message": "Registered PRODIS client '21' with information: be-w-p-obo00159, 169.254.207.71, 00:50:56:B2:40:6B, a_jlambregts, admin, 8040, 2.3 2017-July-05 #1 Release patch 2"
            }
        )

    def test_prodis_general_error(self):
        self.assert_parsing(
            {
                "source": "PRODIS.Error.log",
                "message": "2017-09-20 14:35:38,140  WARN [1] - Catalog structure is not in sync with the database, the number of nodes differs between database '1937' and GUI '1936'."
            },
            {
                "@timestamp": datetime(2017, 9, 20, 14, 35, 38, 140000),
                "level": "WARN",
                "thread": "1",
                "message": "Catalog structure is not in sync with the database, the number of nodes differs between database '1937' and GUI '1936'."
            }
        )

    def test_prodis_config(self):
        self.assert_parsing(
            {
                "source": "PRODIS_Config.log",
                "message": "2017-09-28 15:02:30,667  INFO [1] - Access to the application granted to user admin"
            },
            {
                "@timestamp": datetime(2017, 9, 28, 15, 02, 30, 667000),
                "level": "INFO",
                "thread": "1",
                "message": "Access to the application granted to user admin"
            }
        )

    def test_prodis_rest_client(self):
        self.assert_parsing(
            {
                "source": "ProdisRestClients.log",
                "message": "2017-10-04 11:55:23,224 DEBUG [Asset Propagation Thread] (:0) - <IngestEndTime>2017-10-04T15:00:00Z</IngestEndTime>"
            },
            {
                "@timestamp": datetime(2017, 10, 4, 11, 55, 23, 224000),
                "level": "DEBUG",
                "thread": "Asset Propagation Thread",
                "message": "<IngestEndTime>2017-10-04T15:00:00Z</IngestEndTime>"
            }
        )

    def test_prodis_rest_service(self):
        self.assert_parsing(
            {
                "source": "ProdisRestServices.log",
                "message": """2017-10-04 14:06:44,093 DEBUG [23] (:0) - <No incoming message>"""
            },
            {
                "@timestamp": datetime(2017, 10, 4, 14, 06, 44, 93000),
                "level": "DEBUG",
                "thread": "23",
                "message": "<No incoming message>"
            }
        )

    def test_prodis_reporting_rest_service(self):
        self.assert_parsing(
            {
                "source": "ProdisReportingRestServices.log",
                "message": """2017-10-04 14:06:44,093 DEBUG [23] (:0) - <No incoming message>"""
            },
            {
                "@timestamp": datetime(2017, 10, 4, 14, 06, 44, 93000),
                "level": "DEBUG",
                "thread": "23",
                "message": "<No incoming message>"
            }
        )
