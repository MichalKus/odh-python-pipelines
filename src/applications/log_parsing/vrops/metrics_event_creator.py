from common.log_parsing.dict_event_creator.event_creator import EventCreator


class MetricsEventCreator(EventCreator):
    """
    Extended EventCreator with parsing influx metric str inside msg
    """

    def create(self, row):
        """
        Parse row with Parser and then parse metrics
        :param row: Row from kafka topic
        :return: list of all fields with all URL parameters
        """
        values = EventCreator.create(self, row)
        if values:
            self.custom_dict(values)
        return values

    @staticmethod
    def custom_dict(values):
        """
        convert influx str to dict
        :param values: input dict
        """
        metrics = values["metrics"]
        pairs = []
        for x in metrics.split(','):
            key = x.split('=')[0]
            try:
                value = float(x.split('=')[1])
            except ValueError:
                value = x.split('=')[1]
            pairs.append((key,value))
        res = dict(pairs)
        values.update({"metrics": res})
