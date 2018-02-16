import sys
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils
from pyspark.sql.functions import from_json, lit, col, concat, regexp_replace, split
from pyspark.sql.types import ArrayType, StringType, DoubleType, BooleanType, StructType, StructField

class VmCloudmapCorrelation(object):
    """
    Tie VM-EPG-Tenant mapping with VROPS VM metrics stream
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self.kafka_output = configuration.property("kafka.topics.output")

    def create(self, read_stream):
        """
        Create final stream to output to kafka
        :param read_stream:
        :return: Kafka stream
        """
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
            StructField("metrics", StructType([
                StructField("demandpct", DoubleType()),
                StructField("idlepct", DoubleType()),
                StructField("readypct", DoubleType()),
                StructField("swapwaitpct", DoubleType()),
                StructField("usage_average", DoubleType())
            ])),
            StructField("group", StringType()),
            StructField("name", StringType()),
            StructField("timestamp", StringType()),
            StructField("res_kind", StringType())
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
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
