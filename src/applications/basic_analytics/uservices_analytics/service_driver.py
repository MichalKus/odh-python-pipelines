import sys
from common.kafka_pipeline import KafkaPipeline
from util.utils import Utils
from pyspark.sql.functions import from_json, lit, col, concat, regexp_replace
from pyspark.sql.types import ArrayType, StringType, DoubleType, BooleanType, StructType, StructField

class MicroServices(object):
    """
    https://www.wikitechy.com/tutorials/linux/how-to-calculate-the-cpu-usage-of-a-process-by-pid-in-Linux-from-c
    """

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration\
            .property("analytics.componentName")\
            .replace('<country>', ',')\
            .replace('<service>', ',').split(',')
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

    def _http_jvm_stats(self, stream):
        """
        Filter and process only http and jvm stats for every service.
        :param stream:
        :return: carbon output stream
        """
        inbound = ['jetty_requests', 'jetty_requests_count', 'jetty_responses_total']
        outbound = ['org_apache_http_client_HttpClient_requests_count',
                   'org_apache_http_client_HttpClient_responses_total']
        jvm = ['jvm_memory_heap_used', 'jvm_memory_non_heap_used', 'jvm_gc_PS_MarkSweep_time',
               'jvm_gc_PS_Scavenge_time', 'jvm_gc_PS_Scavenge_count', 'jvm_gc_PS_MarkSweep_count']
        jvm_w = ['%jvm_threads_%', '%jvm_memory_pools_%']

        http_jvm_stream = stream \
            .where(col('metric').isin(inbound + outbound + jvm) | (col('metric').like(jvm_w[0])) | (
                    col('metric').like(jvm_w[1]))) \
            .fillna({'target': 'na', 'code': 'na', 'quantile': 'na'}) \
            .withColumn("metric_name",
                        concat(lit(self._component_name[0]), col("country"), lit(self._component_name[1]),
                               col("service"), lit(".pod."), col("pod_name"), lit("."),
                               col("instance"), lit("."), col("metric"), lit("."), col("target"), lit("."),
                               col("code"), lit("."), col("quantile"), lit("."), col("metric"))) \
            .withColumn('metric_name', regexp_replace('metric_name', '.na', ''))

        return http_jvm_stream

    def _process_pipeline(self, stream):
        """
        Pipeline method
        :param json_stream: kafka stream reader
        :return: list of streams
        """
        formatted = stream \
            .withColumn("country", col("labels").getItem('kubernetes_namespace')) \
            .withColumn("service", col("labels").getItem('app')) \
            .withColumn("pod_name", col('labels').getItem('kubernetes_pod_name')) \
            .withColumn("metric", col("labels").getItem('__name__')) \
            .withColumn("instance", col('labels').getItem('instance')) \
            .withColumn("code", col('labels').getItem('code')) \
            .withColumn("target", col('labels').getItem('target')) \
            .withColumn("quantile", col('labels').getItem('quantile')) \
            .drop("labels") \
            .where((col("country") != "kube-system") | (col("country") != None))

        stream = self._http_jvm_stats(formatted)

        return [stream]

    @staticmethod
    def get_message_schema():
        return StructType([
            StructField("timestamp", StringType()),
            StructField("value", DoubleType()),
            StructField("labels", StructType([
                StructField("kubernetes_namespace", StringType()),
                StructField("app", StringType()),
                StructField("__name__", StringType()),
                StructField("kubernetes_pod_name", StringType()),
                StructField("instance", StringType()),
                StructField("code", StringType()),
                StructField("target", StringType()),
                StructField("quantile", StringType())
            ]))
        ])

def create_processor(configuration):
    return MicroServices(configuration, MicroServices.get_message_schema())


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        create_processor(configuration)
    ).start()
