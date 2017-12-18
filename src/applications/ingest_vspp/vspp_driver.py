import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from util.utils import Utils

from applications.ingest_vspp.vspp_udf import VsppUDF


def create_spark_session(configuration):
    return SparkSession.builder \
        .appName(configuration.property("spark.appName")) \
        .master(configuration.property("spark.master")) \
        .getOrCreate()


def set_kafka_settings(configuration, stream, topic_option, topic, ):
    options = configuration.property("kafka")
    return stream.option("kafka.bootstrap.servers", options["bootstrap.servers"]) \
        .option(topic_option, topic) \
        .option("startingOffsets", options["startingOffsets"]) \
        .option("kafka.security.protocol", options["security.protocol"]) \
        .option("kafka.sasl.mechanism", options["sasl.mechanism"])


def create_read_stream(configuration, spark):
    readStream = spark.readStream.format("kafka")
    return set_kafka_settings(configuration, readStream, "subscribe", configuration.property("kafka")["topic.input"]) \
        .load()


def create_pipeline(read_stream):
    schema = StructType().add("message", StringType())
    result_schema = ArrayType(StructType([
        StructField("@timestamp", TimestampType(), False),
        StructField("processing_speed", DoubleType(), False)
    ]))
    vspp_udf = udf(VsppUDF.process, result_schema)
    return read_stream \
        .select(from_json(read_stream["value"].cast("string"), schema).alias("value")) \
        .withColumn("result", explode(vspp_udf("value.message"))) \
        .select("result.@timestamp", "result.processing_speed") \
        .withColumn("type", lit("airflow_ingest_network_performance_vspp")) \
        .selectExpr("to_json(struct(*)) AS value")


def create_write_stream(configuration, pipeline):
    write_stream = pipeline.writeStream.format("kafka")
    return set_kafka_settings(configuration, write_stream, "topic", configuration.property("kafka")["topic.output"]) \
        .option("checkpointLocation", configuration.property("spark.checkpointLocation"))


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    spark = create_spark_session(configuration)
    read_stream = create_read_stream(configuration, spark)
    pipeline = create_pipeline(read_stream)
    write_stream = create_write_stream(configuration, pipeline)
    write_stream.start().awaitTermination()
