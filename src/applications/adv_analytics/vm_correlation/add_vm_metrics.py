from kafka import KafkaConsumer, TopicPartition
import json
import signal

class IngestVMData(object):
    """
    Takes an input stream grouped by key (hostname) adn returns a flattened stream
    with value holding component metrics tied with vm metrics.
    """
    def __init__(self):
        """
        Fetch VM json message from topic
        :param conf: (dict) topic name
        """
        super(IngestVMData, self).__init__()

    @staticmethod
    def handler(signum, frame):
        raise ValueError

    @staticmethod
    def include_vm(iter, bootstrap_servers, topic_name, LOGGER=False):
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        zk_res = iter[1]
        try:
            signal.signal(signal.SIGALRM, IngestVMData.handler)
            signal.alarm(3)
            partition = TopicPartition(topic_name, zk_res['partition'])
            consumer.assign([partition])
            consumer.seek(partition, zk_res['offset'])
            metrics = json.loads(consumer.next().value)
        except:
            if LOGGER:
                LOGGER.warn("failed to fetch vm metrics from topic")
            metrics = {}
        signal.alarm(0)
        return (iter[0], metrics)

    @staticmethod
    def flatten(elem):
        vm = elem[0]
        iter = elem[1][0]
        metrics = dict(elem[1][1])
        res = []
        for record in iter:
            msg = dict(record)
            msg["vm_metrics"] = metrics
            res.append((vm, msg))
        return res

    @staticmethod
    def filter_correlated(msg):
        if msg[1]["vm_metrics"] == {}:
            return False
        else:
            return True
