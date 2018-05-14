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

        uris = trace_events \
            .where("message like '%HTTP request received from%'") \
            .withColumn("message", regexp_replace("message", r"(>\n\s<)", "><")) \
            .withColumn("message", regexp_replace("message", r"(\n\t)", "\t")) \
            .withColumn("message", regexp_replace("message", r"(\n\s)", " ")) \
            .withColumn("header", explode(split(lower(col("message")), "\n"))) \
            .where("header like 'uri%'") \
            .select(col("hostname"), col("@timestamp"), col("level"), col("message"),
                    regexp_extract("header", ".*?=(.*)", 1).alias("uri"))

        return [self.__info_or_warn_count(info_events, warn_events), self.__error_count(error_events),
                self.__hostname_unique_count(read_stream), self.__hostname_level_count(read_stream),
                self.__tva_full_ingest(read_stream), self.__tva_delta_ingest(read_stream),
                self.__tva_expiry_check(read_stream), self.__tva_ingest_completed(read_stream),
                self.__started_service(info_events), self.__stopped_service(info_events),
                self.__tva_ingest_error(warn_events), self.__customer_provisioning_error(warn_events),
                self.__undefined_warnings(warn_events), self.__customer_provisioning_new(uris),
                self.__customer_provisioning_update(uris), self.__customer_provisioning_delete(uris)]

    def __info_or_warn_count(self, info_events, warn_events):
        return info_events.union(warn_events) \
            .aggregate(Count(aggregation_name=self._component_name + ".info_or_warn"))

    def __error_count(self, error_events):
        return error_events \
            .aggregate(Count(aggregation_name=self._component_name + ".error"))

    def __hostname_unique_count(self, read_stream):
        return read_stream. \
            aggregate(DistinctCount(aggregation_field="hostname", aggregation_name=self._component_name))

    def __hostname_level_count(self, read_stream):
        return read_stream \
            .aggregate(Count(group_fields=["level", "hostname"], aggregation_name=self._component_name))

    def __customer_provisioning_delete(self, uris):
        return uris \
            .where("uri like '%/traxis/householdupdatenotification.traxis?action=delete%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_delete"))

    def __customer_provisioning_update(self, uris):
        return uris \
            .where("uri like '%/traxis/householdupdate?action=update%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_update"))

    def __customer_provisioning_new(self, uris):
        return uris \
            .where("uri like '%/traxis/householdupdate?action=new%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_new"))

    def __undefined_warnings(self, warn_events):
        return warn_events.where(
            "message not like '%Unable to use alias%because alias is already used by%' and "
            "message not like '%One or more validation errors detected during tva ingest%'"
        ).aggregate(Count(group_fields=["hostname"],
                          aggregation_name=self._component_name + ".undefined_warnings"))

    def __customer_provisioning_error(self, warn_events):
        return warn_events \
            .where("message like '%Unable to use alias%because alias is already used by%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".customer_provisioning_error"))

    def __tva_ingest_error(self, warn_events):
        return warn_events \
            .where("message like '%One or more validation errors detected during tva ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_ingest_error"))

    def __stopped_service(self, info_events):
        return info_events \
            .where("message like '%Service - Traxis Service Stopped%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".stopped_service"))

    def __started_service(self, info_events):
        return info_events \
            .where("message like '%Service - Traxis Service Started%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".started_service"))

    def __tva_ingest_completed(self, read_stream):
        return read_stream \
            .where("message like '%Tva ingest completed%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_ingest_completed"))

    def __tva_expiry_check(self, read_stream):
        return read_stream \
            .where("message like '%Expired items detected%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_expiry_check"))

    def __tva_delta_ingest(self, read_stream):
        return read_stream \
            .where("message like '%Starting delta ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_delta_ingest_initiated"))

    def __tva_full_ingest(self, read_stream):
        return read_stream \
            .where("message like '%Starting full ingest%'") \
            .aggregate(Count(group_fields=["hostname"],
                             aggregation_name=self._component_name + ".tva_full_ingest_initiated"))

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
