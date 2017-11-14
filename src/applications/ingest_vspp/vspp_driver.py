import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from util.utils import Utils

from applications.ingest_vspp.vspp_udf import VsppUDF


def createSparkSession(configuration):
    return SparkSession.builder \
        .appName(configuration.property("spark.appName")) \
        .master(configuration.property("spark.master")) \
        .getOrCreate()


def setKafkaSettings(configuration, stream, topicOption, topic, ):
    options = configuration.property("kafka")
    return stream.option("kafka.bootstrap.servers", options["bootstrap.servers"]) \
        .option(topicOption, topic) \
        .option("startingOffsets", options["startingOffsets"]) \
        .option("kafka.security.protocol", options["security.protocol"]) \
        .option("kafka.sasl.mechanism", options["sasl.mechanism"])


def createReadStream(configuration, spark):
    readStream = spark.readStream.format("kafka")
    return setKafkaSettings(configuration, readStream, "subscribe", configuration.property("kafka")["topic.input"]) \
        .load()


def createPipeline(readStream):
    schema = StructType().add("message", StringType())
    resultSchema = ArrayType(StructType([
        StructField("@timestamp", TimestampType(), False),
        StructField("processing_speed", DoubleType(), False)
    ]))
    vsppUdf = udf(VsppUDF.process, resultSchema)
    return readStream \
        .select(from_json(readStream["value"].cast("string"), schema).alias("value")) \
        .withColumn("result", explode(vsppUdf("value.message"))) \
        .select("result.@timestamp", "result.processing_speed") \
        .withColumn("type", lit("airflow_ingest_network_performance_vspp")) \
        .selectExpr("to_json(struct(*)) AS value")


def createWriteStream(configuration, pipeline):
    writeStream = pipeline.writeStream.format("kafka")
    return setKafkaSettings(configuration, writeStream, "topic", configuration.property("kafka")["topic.output"]) \
        .option("checkpointLocation", configuration.property("spark.checkpointLocation"))


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    spark = createSparkSession(configuration)
    readStream = createReadStream(configuration, spark)
    pipeline = createPipeline(readStream)
    writeStream = createWriteStream(configuration, pipeline)
    writeStream.start().awaitTermination()
