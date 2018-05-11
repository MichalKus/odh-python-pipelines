"""
The module for the driver to calculate metrics related to Traxis Backend general component.
"""
from pyspark.sql.functions import col, regexp_replace, regexp_extract, explode, split, lower
from pyspark.sql.types import StructField, StructType, TimestampType, StringType

from common.basic_analytics.aggregations import Count, DistinctCount
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


class TraxisBackendGeneral(BasicAnalyticsProcessor):
    """
    The processor implementation to calculate metrics related to Traxis Backend general component.
    """

    def _process_pipeline(self, read_stream):
        info_events = read_stream.where("level == 'INFO'")
        warn_events = read_stream.where("level == 'WARN'")
        trace_events = read_stream.where("level == 'TRACE'")
        error_events = read_stream.where("level == 'ERROR'")

        info_or_warn_count = info_events.union(warn_events) \
            .aggregate(Count(aggregation_name=self._component_name + ".info_or_warn"))

        error_count = error_events \
            .aggregate(Count(aggregation_name=self._component_name + ".error"))

        hostname_unique_count = read_stream. \
            aggregate(DistinctCount(aggregation_field="hostname", aggregation_name=self._component_name))

        hostname_level_count = read_stream \
            .aggregate(Count(group_fields=["hostname", "level"], aggregation_name=self._component_name))

        tva_full_ingest = read_stream \
            .where("message like '%Starting full ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_full_ingest_initiated"))

        tva_delta_ingest = read_stream \
            .where("message like '%Starting delta ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_delta_ingest_initiated"))

        tva_expiry_check = read_stream \
            .where("message like '%Expired items detected%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_expiry_check"))

        tva_ingest_completed = read_stream \
            .where("message like '%Tva ingest completed%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_ingest_completed"))

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
            .withColumn("message", regexp_replace("message", r"(>\n\s<)", "><")) \
            .withColumn("message", regexp_replace("message", r"(\n\t)", "\t")) \
            .withColumn("message", regexp_replace("message", r"(\n\s)", " ")) \
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

        return [info_or_warn_count, error_count, tva_expiry_check, tva_delta_ingest, tva_full_ingest,
                tva_ingest_completed, started_service,
                stopped_service, tva_ingest_error, customer_provisioning_error, undefined_warnings,
                customer_provisioning_new, customer_provisioning_update, customer_provisioning_delete,
                hostname_unique_count, hostname_level_count]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("level", StringType()),
            StructField("message", StringType()),
            StructField("hostname", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return TraxisBackendGeneral(configuration, TraxisBackendGeneral.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
