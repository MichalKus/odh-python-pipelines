import socket
import json

class CarbonSink(object):
    """
    Takes an input stream grouped by key (hostname) adn returns a flattened stream
    with value holding component metrics tied with vm metrics.
    """
    def __init__(self, config, LOGGER=False):
        """
        Fetch VM json message from topic
        :param conf: (dict) topic name
        """
        super(CarbonSink, self).__init__()
        self.carbon_server = config.property('graphite.carbonServers')
        self.carbon_port = config.property('graphite.carbonPort')
        self.LOGGER = LOGGER

    def send_partition(self, iter):
        sock = socket.socket()
        sock.connect((self.carbon_server, self.carbon_port))
        message = ''
        for record in iter:
            s = "{} {} {}".format(record['metric_path'], record['value'], record['timestamp'])
            message = message + s + '\n'
        sock.sendall(message)
        sock.close()
