import sys
from collections import namedtuple

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.kafka_pipeline import KafkaPipeline
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.basic_analytics.aggregations import Count
from common.spark_utils.custom_functions import custom_translate
from util.utils import Utils

QueryFilter = namedtuple("QueryFilter", "level message name")


class NokiaVrmSchedulerBSAudit(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        query_filters = [
            QueryFilter("ERROR", "HTTP message for accessing Scheduling Adapter BS not readable", "HTTP_not_readable"),
            QueryFilter("ERROR", "UserProfile not found", "Userprofile_not_found"),
            QueryFilter("ERROR", "UserProfile provisioned", "Userprofile_not_found"),
            QueryFilter("ERROR", "Error parsing Scheduling Adapter XML body", "XML_parsing_error"),

            QueryFilter("ERROR",
                        "Response from request to Traxis to get user entitlement for given channel contains several Product items",
                        "Several_products_traxis_get_user_response"),
            QueryFilter("ERROR",
                        "Response from request to Traxis to get UpcCasProductId contains several Product items",
                        "Several_products_traxis_get_product_id_response"),
            QueryFilter("ERROR", "Middleware (Traxis 5F interface). Error returned (response code = 500)", "500_error"),
            QueryFilter("WARN", "Middleware (Traxis 5F interface). Error returned when checking entitlement",
                        "Checking_entitlement_error"),
            QueryFilter("WARN", "Middleware (Traxis 5F interface). Error returned when getting profile",
                        "Getting_profile_error"),
            QueryFilter("ERROR", "Unable to parse Traxis event date format", "Unable_parse_traxis_date_format_error"),
            QueryFilter("ERROR", "No UpcCasProductId retured by Traxis", "No_product_id_returned_error"),
            QueryFilter("ERROR", "Failed to retrieve events from Traxis", "Failed_retrieve_events_error"),
            QueryFilter("ERROR", "No Channel found in Traxis 5F interface", "No_channel_found_error"),
            QueryFilter("INFO", "Failed to check user barring", " Failed_check_user_barring_events_error"),
            QueryFilter("WARN", "Invalid parameter CustormerId in Traxis 5F interface",
                        "Invalid_customer_id_in_traxis_interface"),
            QueryFilter("WARN", "Traxis event field NetworkRecordingLicenseDuration should be expressed in days",
                        "NetworkRecordingLicenceDuration_should_be_in_days"),
            QueryFilter("WARN", "Getting different channel guard times from Traxis that the provisioned in the system",
                        "Get_different_channel_guard_time")
        ]

        query = " or ".join(map(lambda
                                    query_filter: "(level == '" + query_filter.level + "' and message like '%" + query_filter.message + "%')",
                                query_filters))
        return [read_stream
                    .where(query)
                    .withColumn("message_type",
                                custom_translate(
                                    source_field=col("message"),
                                    mapping=dict(
                                        (".*" + query_filter.message + ".*", query_filter.name) for query_filter in
                                        query_filters),
                                    default_value="unclassified")) \
                    .aggregate(Count(group_fields=["level", "message_type"],
                                     aggregation_name=self._component_name))
                ]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("event_id", StringType()),
            StructField("domain", StringType()),
            StructField("ip", StringType()),
            StructField("method", StringType()),
            StructField("params", StringType()),
            StructField("description", StringType()),
            StructField("message", StringType())
        ])


def create_processor(configuration):
    return NokiaVrmSchedulerBSAudit(configuration, NokiaVrmSchedulerBSAudit.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
