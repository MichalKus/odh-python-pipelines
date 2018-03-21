from pyspark.sql.functions import lit, udf, struct

from common.log_parsing.log_parsing_processor import LogParsingProcessor


class FlumeLogParsingProcessor(LogParsingProcessor):
    """
    A processor implementation to parse messages from Apache Flume,
    """

    def create(self, read_stream):
        create_full_event_udf = udf(self._create_full_event, self._get_udf_result_schema())
        return [
            read_stream.select(
                struct(
                    struct(lit("").alias("hostname")).alias("beat"),
                    lit("").alias("topic"),
                    lit("").alias("source"),
                    read_stream["value"].cast("string").alias("message")
                ).alias("json")
            )
            .select(create_full_event_udf("json").alias("result"))
            .selectExpr("result.topic AS topic", "result.json AS value")]
