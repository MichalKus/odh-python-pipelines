from datetime import datetime

from applications.log_parsing.poster_server.driver import create_event_creators
from common.log_parsing.timezone_metadata import timezones
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase
from util.configuration import Configuration


class PosterServerMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators(Configuration(dict={"timezone": {"name": "Europe/Amsterdam"}}))

    def test_parse_ericsson_transcoder_log_started(self):
        self.assert_parsing(
            {
                'source': 'EricssonTranscoder.log',
                'message': "{\"diagToolServersBasePort\":0,\"elapsedTime\":764849,\"endTime\":-1,\"inputs/0/url\":\"file:///mnt/obo_manage/Countries/NL/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2F37500~~2FAZIA1000000000714972/619905_MOV_CZIA0000000000714972.ts\",\"jobId\":\"6eed527011a544db92c721dd502f2dc0\",\"licensing\":{\"state\":\"GRANTED\",\"tokens\":[{\"grantedCount\":15,\"name\":\"FAT1023464/81\",\"neededCount\":15}]},\"outputs/0/publishingPoint\":\"file:///mnt/obo_manage/Countries/NL/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2F37500~~2FAZIA1000000000714972/ff5ace7c77700e80641fc84dbd5d455d_639d83a293426dc0af0aa62a2d41c217\",\"outputs/0/transport/0/filename\":\"HEVC-576p25-STB_01_576p25.ts\",\"outputs/0/transport/1/filename\":\"HEVC-576p25-STB_02_576p25.ts\",\"startTime\":1524042480035,\"state\":{\"additionalInformation\":\"31.86% of encoding task completed\",\"assetDuration\":836480,\"completion\":\"31.86\",\"message\":\"\",\"status\":\"started\"},\"taskId\":\"encoding\"}"
            },
            {
                '@timestamp': datetime(2018, 4, 18, 9, 20, 44, 0).replace(tzinfo=timezones["Europe/Amsterdam"]),
                'status': 'started',
                'crid': 'crid~~3A~~2F~~2Fog.libertyglobal.com~~2F37500~~2FAZIA1000000000714972',
                'country': 'NL',
                'jobId': '6eed527011a544db92c721dd502f2dc0',
                'startTime': '1524042480035',
                'elapsedTime': '764849',
                'completion': '31.86',
                'assetDuration': '836480',
                'additionalInformation': '31.86% of encoding task completed',
                'message': ''
            }
        )

    def test_parse_ericsson_transcoder_log_done(self):
        self.assert_parsing(
            {
                'source': 'EricssonTranscoder.log',
                'message': "{\"diagToolServersBasePort\":0,\"elapsedTime\":3791577,\"endTime\":1524043250184,\"inputs/0/url\":\"file:///mnt/obo_manage/Countries/BE/FromAirflow/crid~~3A~~2F~~2Ftelenet.be~~2Fac885b69-ac8e-4723-8399-fe9aa63cfb81/ce1__48d__ce148d3a-e2bb-4c46-94ce-1d4d1a656849__TN00030592_08_0010_A0_HD_HD_S_OSNL_A.ts\",\"jobId\":\"394fb679fc354b65802645081b1fb00b\",\"licensing\":{\"state\":\"RELEASED\",\"tokens\":[]},\"outputs/0/publishingPoint\":\"file:///mnt/obo_manage/Countries/BE/FromAirflow/crid~~3A~~2F~~2Ftelenet.be~~2Fac885b69-ac8e-4723-8399-fe9aa63cfb81/02b2d106d9a77360b68d8e101e419578_385ed59267bc3781d5e463264e5d637e\",\"outputs/0/transport/0/filename\":\"HEVC-1080p25-STB_01_1080p25.ts\",\"outputs/0/transport/1/filename\":\"HEVC-1080p25-STB_02_576p25.ts\",\"startTime\":1524039458607,\"state\":{\"additionalInformation\":\"100.00% of encoding task completed\",\"assetDuration\":2496880,\"completion\":\"100.00\",\"message\":\"\",\"status\":\"done\"},\"taskId\":\"encoding\"}"
            },
            {
                '@timestamp': datetime(2018, 4, 18, 9, 20, 50, 0).replace(tzinfo=timezones["Europe/Amsterdam"]),
                'status': 'done',
                'crid': 'crid~~3A~~2F~~2Ftelenet.be~~2Fac885b69-ac8e-4723-8399-fe9aa63cfb81',
                'country': 'BE',
                'jobId': '394fb679fc354b65802645081b1fb00b',
                'startTime': '1524039458607',
                'elapsedTime': '3791577',
                'completion': '100.00',
                'assetDuration': '2496880',
                'additionalInformation': '100.00% of encoding task completed',
                'message': ''
            }
        )

    def test_parse_ericsson_transcoder_log_error(self):
        self.assert_parsing(
            {
                'source': 'PosterServer.Error.log',
                'message': "{\"diagToolServersBasePort\":0,\"elapsedTime\":460943,\"endTime\":-1,\"inputs/0/url\":\"file://mnt/obo_manage/Countries/NL/FromAirflow/crid~~3A~~2F~~2Fog.libertyglobal.com~~2F37500~~2FAZIA1000000000714972/619905_MOV_CZIA0000000000714972.ts\",\"jobId\":\"229c3ae31bec455a8cab8b088a3e6998\",\"licensing\":{\"state\":\"GRANTED\",\"tokens\":[{\"grantedCount\":14,\"name\":\"FAT1023464/81\",\"neededCount\":14}]},\"outputs/0/publishingPoint\":\"file://172.30.177.206/lab5_managed/airflow_aws/crid~~3A~~2F~~2Fog.libertyglobal.com~~2F1001~~2Fts0212-20180126T170000CETpt/afa325ca2ac3d285bad33ef9d7ceb8e8_786490661993fb99a6ecef65b225fc9e\",\"outputs/0/transport/0/filename\":\"HEVC-576i25-STB_01_576i25.ts\",\"outputs/0/transport/1/filename\":\"HEVC-576i25-STB_02_576i25.ts\",\"startTime\":1516885706424,\"state\":{\"assetDuration\":532096,\"completion\":\"0.00\",\"message\":\"i/o error\",\"status\":\"error\"},\"taskId\":\"encoding\"}"
            },
            {
                '@timestamp': datetime(2018, 4, 18, 9, 15, 40, 0).replace(tzinfo=timezones["Europe/Amsterdam"]),
                'status': 'error',
                'crid': 'crid~~3A~~2F~~2Fog.libertyglobal.com~~2F37500~~2FAZIA1000000000714972',
                'country': 'NL',
                'jobId': '229c3ae31bec455a8cab8b088a3e6998',
                'startTime': '1524042480037',
                'elapsedTime': '460943',
                'completion': '0.00',
                'assetDuration': '532096',
                'additionalInformation': '',
                'message': 'i/o error'
            }
        )
