import re
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
        split = re.split(r'[, ]', metrics)
        res =[]
        for x in split:
            if '=' in x:
                res.append(x)
            else:
                try:
                    res[-1] = res[-1] + x
                except IndexError:
                    pass
        for x in res:
            [key, metric_value] = x.split('=')
            try:
                value = float(metric_value)
            except ValueError:
                value = metric_value
            pairs.append((key,value))
        res = dict(pairs)
        values.update({"metrics": res})