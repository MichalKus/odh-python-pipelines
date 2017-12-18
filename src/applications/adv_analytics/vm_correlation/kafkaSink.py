from kafka import KafkaProducer, errors
import json

class KafkaOutput(object):
    """
    Takes an input stream grouped by key (hostname) adn returns a flattened stream
    with value holding component metrics tied with vm metrics.
    """
    def __init__(self, config, LOGGER=False):
        """
        Fetch VM json message from topic
        :param conf: (dict) topic name
        """
        super(KafkaOutput, self).__init__()
        self.config = config
        self.LOGGER = LOGGER

    def send_partition(self, iter):
        topic = self.config.property('kafka.topicOutput')
        producer = KafkaProducer(bootstrap_servers = self.config.property('kafka.bootstrapServers').split(','))
        for record in iter:
            try:
                record_metadata = producer.send(topic, json.dumps(record[1]).encode('utf-8')).get(timeout=30)
            except errors.KafkaTimeoutError:
                if self.LOGGER:
                    self.LOGGER.warn("Topic: {}, Failed to deliver message".format(topic))
        producer.flush()
