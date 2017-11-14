import sys
from util.utils import Utils

from pyspark.sql.types import *
from pyspark.sql.functions import *

from applications.basic_analytics.basic_analytics import BasicAnalytics


def create_schema():
    return StructType([
        StructField("@timestamp", TimestampType()),
        StructField("sr_method", StringType()),
        StructField("web_object", StringType()),
        StructField("response_code", StringType()),
        StructField("r_ip", StringType()),
    ])


def count_get_metric(field1, field2):
    def inner_function(json_stream):
        return json_stream.where("json.sr_method == 'GET'") \
            .where("json." + field1 + "!= 'unclassified'") \
            .where("json." + field2 + "!= 'unclassified'") \
            .select(json_stream["json.@timestamp"],
                    concat(lit("heapp.ida.lab5aobo.http_proxy.get." + field1 + "."),
                           json_stream["json." + field1],
                           lit("." + field2 + "."),
                           json_stream["json." + field2],
                           lit(".count")).alias("metric")
                    ) \
            .withColumn("value", lit("1"))

    return inner_function


if __name__ == "__main__":
    BasicAnalytics(Utils.loadConfig(sys.argv[:]), create_schema(), [
        count_get_metric("response_code", "web_object"),
        count_get_metric("response_code", "r_ip")
    ]).start()
