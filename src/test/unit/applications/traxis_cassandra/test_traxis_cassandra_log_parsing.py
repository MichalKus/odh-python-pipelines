from datetime import datetime

from applications.log_parsing.traxis.traxis_cassandra import create_event_creators
from test.unit.core.base_message_parsing_test_cases import BaseMultipleMessageParsingTestCase


class TraxisCassandraMessageParsingTestCase(BaseMultipleMessageParsingTestCase):
    event_creators = create_event_creators()

    def test_traxis_cassandra(self):
        test_cases = [(
            {
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_cassandra_log_err_v1",
                "message": "ServiceHost: 2016-11-08 08:05:05,490 ERROR [92] MaintenanceController - Eventis.Cassandra.Service.CassandraServiceException+HostGeneralException: Error from nodetool: Keyspace [Traxis] does not exist."
            }, {
                "@timestamp": datetime(2016, 11, 8, 8, 5, 5, 490000),
                "level": "ERROR",
                "message": "MaintenanceController - Eventis.Cassandra.Service.CassandraServiceException+HostGeneralException: Error from nodetool: Keyspace [Traxis] does not exist."
            }
        ), (
            {
                "topic": "vagrant_in_eosdtv_be_prd_heapp_traxis_cassandra_log_gen_v1",
                "message": "Cassandra: Eventis.Cassandra.Service.CassandraServiceException+CassandraWarningException: WARN [main] 2017-01-20 08:11:39,729 No host ID found, created 3e901daf-d150-4e40-ba33-bc09b9c04158 (Note: This should happen exactly once per node)."
            }, {
                "@timestamp": datetime(2017, 1, 20, 8, 11, 39, 729000),
                "level": "WARN",
                "message": "No host ID found, created 3e901daf-d150-4e40-ba33-bc09b9c04158 (Note: This should happen exactly once per node)."
            }
        )]

        for test_message, parsed_message in test_cases:
            self.assert_parsing(test_message, parsed_message)
