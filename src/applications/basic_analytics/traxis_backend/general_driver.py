import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.basic_analytics.aggregations import Count
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils


class TraxisBackendGeneral(BasicAnalyticsProcessor):
    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        trace_events = read_stream.where("level == 'TRACE'")

        tva_success_ingest = info_events \
            .where("message like '%Tva ingest completed%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_success_ingest"))

        tva_delta_ingest = info_events \
            .where("message like '%New sequence number%is different from current%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_delta_ingest"))

        tva_full_ingest = info_events \
            .where("message like '%[Task = TvaManagementFullOnlineIngest] Starting full ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_full_ingest"))

        started_service = info_events \
            .where("message like '%Service - Traxis Service Started%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".started_service"))

        stopped_service = info_events \
            .where("message like '%Service - Traxis Service Stopped%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".stopped_service"))

        tva_ingest_error = warn_events \
            .where("message like '%One or more validation errors detected during tva ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_ingest_error"))

        customer_provisioning_error = warn_events \
            .where("message like '%Unable to use alias%because alias is already used by%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_error"))

        undefined_warnings = warn_events.where(
            "message not like '%Unable to use alias%because alias is already used by%' and "
            "message not like '%One or more validation errors detected during tva ingest%'"
        ).aggregate(Count(group_fields=["hostname"],
                          aggregation_name=self._component_name + ".undefined_warnings"))

        uris = trace_events \
            .where("message like '%HTTP request received from%'") \
            .withColumn("message", regexp_replace("message", "(>\n\s<)", "><")) \
            .withColumn("message", regexp_replace("message", "(\n\t)", "\t")) \
            .withColumn("message", regexp_replace("message", "(\n\s)", " ")) \
            .withColumn("header", explode(split(lower(col("message")), "\n"))) \
            .where("header like 'uri%'") \
            .select(col("hostname"), col("@timestamp"), col("level"), col("message"),
                    regexp_extract("header", ".*?=(.*)", 1).alias("uri"))

        customer_provisioning_new = uris \
            .where("uri like '%/traxis/householdupdate?action=new%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_new"))

        customer_provisioning_update = uris \
            .where("uri like '%/traxis/householdupdate?action=update%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_update"))

        customer_provisioning_delete = uris \
            .where("uri like '%/traxis/householdupdatenotification.traxis?action=delete%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_delete"))

        return [tva_success_ingest, tva_delta_ingest, tva_full_ingest, started_service,
                stopped_service, tva_ingest_error, customer_provisioning_error, undefined_warnings,
                customer_provisioning_new, customer_provisioning_update,
                customer_provisioning_delete]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    return TraxisBackendGeneral(configuration, TraxisBackendGeneral.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
