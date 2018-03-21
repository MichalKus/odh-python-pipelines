from pyspark.sql.functions import struct

from common.log_parsing.log_parsing_processor import LogParsingProcessor


class FlumeLogParsingProcessor(LogParsingProcessor):
    """
    A processor implementation to parse messages from Apache Flume,
    """

    def _extract_json(self, stream):
        return stream.select(struct(stream["value"].cast("string").alias("message")).alias("json"))

    @staticmethod
    def _enrich_result(result, row):
        return result
