from pyspark.streaming import StreamingContext
from kafka import SimpleClient
from pyspark.streaming.kafka import KafkaUtils

class KafkaConnector(object):
    """
    RDD stream from kafka topic
    """
    def __init__(self, conf):
        """
        Obtain all options from config file and build kafka producer/consumer.
        :param conf: (dict) topic name
        """
        super(KafkaConnector, self).__init__()
        self.topic = conf.property('kafka.topicInput')
        self.bootstrap_server = conf.property('kafka.bootstrapServers')
        self.zookeeper_host = conf.property('kafka.zookeeperHosts').split(',')[0]
        self.auto_offset_reset = conf.property('kafka.autoOffsetReset')
        self.group_id = conf.property('kafka.groupId')
        self.partitions = self._get_partition_ids(self.topic, self.bootstrap_server)

    @staticmethod
    def create_spark_context(config, sc):
        """
        Create the spark context using the correct configurations
        :param config: configurations from .yml file
        :return: spark context
        """
        ssc = StreamingContext(sc, config.property('spark.batchInterval'))
        ssc.checkpoint(config.property('spark.checkpointLocation'))

        return ssc

    def _get_partition_ids(self, topic, bootstrap_server):
        """
        Get the number of partitions for a specific topic.
        :param topic: (string) topic name
        :param bootstrap_servers: (string) single bootstrap server 'host:port'
        :return: (int) no. of partitions
        """
        client = SimpleClient(bootstrap_server)
        topic_partition_ids = client.get_partition_ids_for_topic(topic.encode('utf-8'))

        return len(topic_partition_ids)

    def create_kafka_stream(self, ssc):
        """
        Create input kafka stream
        :param ssc: Spark Streaming Context
        :return: RDD stream
        """
        topic = {self.topic: self.partitions}
        kafkaParams = {}
        kafkaParams["auto.offset.reset"] = self.auto_offset_reset
        input_stream = KafkaUtils.createStream(ssc, self.zookeeper_host, self.group_id, topic, kafkaParams)
        return input_stream
