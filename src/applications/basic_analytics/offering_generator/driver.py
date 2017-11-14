import sys
from util.utils import Utils

from pyspark.sql.types import *
from pyspark.sql.functions import *

from applications.basic_analytics.basic_analytics import BasicAnalytics


def create_schema():
    return StructType([
        StructField("@timestamp", TimestampType()),
        StructField("level", StringType()),
        StructField("provider", StringType()),
        StructField("script", StringType()),
        StructField("message_end", StringType())
    ])


def count_metric(metric, level, message, script=""):
    def inner_function(json_stream):
        return json_stream \
            .where("json.level == '" + level + "' and json.message_end like '" + message + "'"
                   + ("and json.script == '" + script + "'" if script != "" else "")) \
            .select(json_stream["json.@timestamp"],
                    concat(lit("heapp.oferring_generator.lab5aobo." + metric + ".count")).alias("metric")
                    ) \
            .withColumn("value", lit("1"))

    return inner_function


def count_provider_metric(metric, script, message):
    def inner_function(json_stream):
        return json_stream \
            .where("json.script == '" + script + "' and json.message_end like '" + message + "'") \
            .select(json_stream["json.@timestamp"],
                    concat(lit("heapp.oferring_generator.lab5aobo." + metric + ".provider."),
                           regexp_replace(json_stream["json.provider"], "\s", "_"),
                           lit(".count")).alias("metric")
                    ) \
            .withColumn("value", lit("1"))

    return inner_function


if __name__ == "__main__":
    BasicAnalytics(Utils.loadConfig(sys.argv[:]), create_schema(), [
        count_metric("warning", "ERROR", "WARN%", "eventis_ingest-tva.sh"),
        count_metric("error", "ERROR", "%ncftpls: cannot open localhost: remote host refused connection%"),
        count_metric("ingested_tva", "INFO", "%ncftpput%TVA_%", "eventis_ingest-tva.sh"),
        count_metric("content", "INFO", "%Ingested%", "eventis_ingest-tva.sh"),
        count_metric("critical", "ERROR", "%CRITICAL%"),
        count_metric("warn_warning", "ERROR", "%WARN%"),
        count_provider_metric("success", "adi2tva.sh", "%End /home/og/bin/adi2tva.sh"),
        count_provider_metric("exported", "export_og.sh", "%End /home/og/bin/export_og.sh")
    ]).start()
