import sys

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import split
from pyspark.sql.functions import concat
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType

from util.utils import Utils

MONTHS = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

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
    split_col = split(readStream['value'], ' \[')
    message = split_col.getItem(1)
    systemDetails = split(split_col.getItem(0), ' ')
    
    currentYear = datetime.now().year
    month = systemDetails.getItem(1)
    date = systemDetails.getItem(2)
    time = systemDetails.getItem(3)
    source = systemDetails.getItem(4)
    
    fsm = split(split(systemDetails.getItem(5), '%').getItem(1), '-')
    facility = fsm.getItem(0)
    severity = fsm.getItem(1)
    mnemonic = fsm.getItem(2)
    
    udf = UserDefinedFunction(lambda x: MONTHS.get(x), StringType())
    
    return readStream.withColumn('timestamp', concat_ws(' ', concat_ws('-', lit(currentYear), udf(month), date), time).astype(TimestampType())) \
        .withColumn('source', source) \
        .withColumn('facility', facility) \
        .withColumn('severity', severity) \
        .withColumn('mnemonic', mnemonic) \
        .withColumn('message', concat(lit('['), message)) \
        .selectExpr("to_json(struct(timestamp, source, facility, severity, mnemonic, message)) AS value")


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

