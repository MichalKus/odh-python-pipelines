import sys
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils
from pyspark.sql.functions import from_json, lit, col, concat, regexp_replace
from pyspark.sql.types import ArrayType, StringType, DoubleType, BooleanType, StructType, StructField

class MicroServicesNodes(object):
    """
    https://www.wikitechy.com/tutorials/linux/how-to-calculate-the-cpu-usage-of-a-process-by-pid-in-Linux-from-c
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration\
            .property("analytics.componentName")\
            .replace('<instance>', ',').split(',')
        self.kafka_output = configuration.property("kafka.topics.output")

    def create(self, read_stream):

        json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*")

        return [self._convert_to_kafka_structure(dataframe) for dataframe in self._process_pipeline(json_stream)]

    def _convert_to_kafka_structure(self, dataframe):
        return dataframe \
            .selectExpr("to_json(struct(*)) AS value") \
            .withColumn("topic", lit(self.kafka_output))

    def _node_process_stats(self, stream):
        """
        Filter and process only node and process stats for every host.
        :param stream:
        :return: carbon output stream
        """

        node_proc_stream = stream\
            .fillna({'device': 'na', 'fstype': 'na', 'mountpoint': 'na'}) \
            .withColumn("metric_name",
                        concat(lit(self._component_name[0]), col("instance"), lit(".device."), col("device"),
                               lit(".fstype."), col("fstype"), lit(".mountpoint."), col("mountpoint"))) \
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
            .withColumn("device", col('labels').getItem('device')) \
            .withColumn("fstype", col('labels').getItem('fstype')) \
            .withColumn("mountpoint", col('labels').getItem('mountpoint')) \
            .drop("labels") \
            .where((col('metric').like('%node_%')) | (col('metric').like('%process_%')))

        stream = self._node_process_stats(formatted)

        return [stream]

    @staticmethod
    def get_message_schema():
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
    return MicroServicesNodes(configuration, MicroServicesNodes.get_message_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
