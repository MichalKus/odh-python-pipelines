import sys
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils
from pyspark.sql.functions import from_json, lit, col
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

class CorrelationPipeline(KafkaPipeline):
    """
    Extending Kafka Pipeline to add support for reading from hdfs (batch)
    """

    def _create_custom_read_stream(self, spark):
        read_stream = spark.readStream.format("kafka")
        options = self._configuration.property("kafka")
        result = self.__set_kafka_securing_settings(read_stream, options) \
            .option("subscribe", ",".join(self._configuration.property("kafka.topics.inputs")))
        self.__add_option_if_exists(result, options, "maxOffsetsPerTrigger")
        self.__add_option_if_exists(result, options, "startingOffsets")
        self.__add_option_if_exists(result, options, "failOnDataLoss")
        return [spark, result.load()]

class VmCloudmapCorrelation(object):
    """
    Tie VM-EPG-Tenant mapping with VROPS VM metrics stream
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self.kafka_output = configuration.property("kafka.topics.output")

    def create(self, custom_read_stream):
        """
        Create final stream to output to kafka
        :param read_stream:
        :return: Kafka stream
        """
        [spark, read_stream] = custom_read_stream
        cloudmap_df = spark.read.text('file:///spark/checkpoints/cloudmap/cloudmap_mapping.txt')
        # json_schema = spark.read.json(cloudmap_df.rdd.map(lambda row: row.value)).schema
        # cached_df = cloudmap_df.withColumn('json', from_json(col('value'), json_schema))
        # cached_df.show()
        cloudmap_df.show()
        json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*")

        return [self._convert_to_kafka_structure(dataframe) for dataframe in self._process_pipeline(json_stream)]

    def _convert_to_kafka_structure(self, dataframe):
        """
        Convert to json schema and add topic name as output
        :param dataframe:
        :return: output stream
        """
        return dataframe \
            .selectExpr("to_json(struct(*)) AS value") \
            .withColumn("topic", lit(self.kafka_output))

    def _process_pipeline(self, stream):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """

        return [stream]

    @staticmethod
    def create_schema():
        """
        Schema for input stream.
        """
        return StructType([
            StructField("res_kind", StringType()),
            StructField("group", StringType()),
            StructField("name", StringType()),
            StructField("@timestamp", StringType()),
            StructField("metric_name", StringType()),
            StructField("value", DoubleType())
        ])

def create_processor(configuration):
    """
    Build processor using configurations and schema.
    :param configuration:
    :return:
    """
    return VmCloudmapCorrelation(configuration, VmCloudmapCorrelation.create_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    CorrelationPipeline(
        configuration,
        create_processor(configuration)
    ).start()
