import sys
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils
from pyspark.sql.functions import from_json, lit, col, concat, regexp_replace, split
from pyspark.sql.types import ArrayType, StringType, DoubleType, BooleanType, StructType, StructField

class MicroServicesNodes(object):
    """
    Process uservices node and process stats from prometheus and export to graphite.
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration\
            .property("analytics.componentName")\
            .replace('<instance>', ',').split(',')
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

    def _node_process_stats(self, stream):
        """
        Filter and process only node and process stats for every host.
        :param stream:
        :return: carbon output stream
        """

        node_proc_stream = stream \
            .fillna({'device': 'na', 'fstype': 'na', 'mountpoint': 'na'}) \
            .withColumn("metric_name",
                        concat(lit(self._component_name[0]), col("instance"), lit(".device."), col("device"),
                               lit(".fstype."), col("fstype"), lit(".mountpoint."), col("mountpoint"),
                               lit("."), col("metric_group"), lit("."), col("metric_type"), lit("."), col("metric"))) \
            .drop("metric_group") \
            .drop("metric_type") \
            .withColumn('metric_name', regexp_replace('metric_name', '.device.na', '')) \
            .withColumn('metric_name', regexp_replace('metric_name', '.fstype.na', '')) \
            .withColumn('metric_name', regexp_replace('metric_name', '.mountpoint.na', ''))

        return node_proc_stream

    def _process_pipeline(self, stream):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """
        formatted = stream \
            .withColumn("metric", col("labels").getItem('__name__')) \
            .withColumn("instance", col('labels').getItem('instance')) \
            .withColumn('instance', regexp_replace('instance', r'\.', '-')) \
            .withColumn("device", col('labels').getItem('device')) \
            .withColumn('device', regexp_replace('device', r'\.', '-')) \
            .withColumn("fstype", col('labels').getItem('fstype')) \
            .withColumn("mountpoint", col('labels').getItem('mountpoint')) \
            .withColumn('mountpoint', regexp_replace('mountpoint', r'\.', '-')) \
            .drop("labels") \
            .withColumn('metric_group', split(col('metric'), '_').getItem(0)) \
            .withColumn('metric_type', split(col('metric'), '_').getItem(1)) \
            .where((col('metric_group').like('%node%')) | (col('metric_group').like('%process%')))

        stream = self._node_process_stats(formatted)

        return [stream]

    @staticmethod
    def get_message_schema():
        """
        Provide schema for reading input messages.
        :return: schema
        """
        return StructType([
            StructField("timestamp", StringType()),
            StructField("value", DoubleType()),
            StructField("labels", StructType([
                StructField("__name__", StringType()),
                StructField("instance", StringType()),
                StructField("device", StringType()),
                StructField("fstype", StringType()),
                StructField("mountpoint", StringType())
            ]))
        ])

def create_processor(configuration):
    """
    Build processor using configurations and schema.
    :param configuration:
    :return:
    """
    return MicroServicesNodes(configuration, MicroServicesNodes.get_message_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
