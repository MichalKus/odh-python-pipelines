import re
import sys

from applications.base_apps.base_spark_job import BaseSparkJob
from common.extractor import Extractor
from util import Utils

r"""
Common configurable job to calculate duration between any events.
A configuration for the job must be like that:
    
spark:
  appName: Spark application name
  master: local[*]
  checkpointLocation: file:///spark/checkpoints
  batchInterval: 1

kafka:
  options:
    bootstrap.servers: test1:9093
    group.id: group0
    client.id: client0
    auto.offset.reset: smallest
  input:
    topic: input_kafka_topic
  output:
    topic: output_kafka_topic

extractors:
  # Start event extractor (group 1 - date, group 2 - unique id)
  - "(\d{4}-[0-3]\d-[01]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+[+-][0-2]\d:[0-5]\d)\s+.+\s+INFO:\s+.*\s+Recursive\s+retrieve\s+(\S*)"
  # Stop event extractor (group 1 - date, group 2 - unique id)
  - "(\d{4}-[0-3]\d-[01]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+[+-][0-2]\d:[0-5]\d)\s+.+\s+INFO:\s+.*\s+ftp_poll-vmuk-feed\.sh\s+(\S+)\s+retrieved"

expire:
  delay: 60
  event_time: true  # default: 'true'
  behavior: fail  # values: 'keep', 'drop' or 'fail', default: 'fail'

result:
  id_field: package  # default: 'id'
  value_field: some_value  # default: 'value'
  value_unit: ms  # default: 's' (seconds)
  static_fields:
    system: sample_system
    type: sample_type

"""

default_dateformat = "%Y-%m-%d %H:%M:%S,%f"


class DurationJob(BaseSparkJob):
    def __init__(self):
        conf = Utils.load_config(sys.argv[:])
        self.__system = conf.property('system')
        self.__type = conf.property('type')
        props = conf.property('extractors')
        start_extractor = self.extractor(props[0], "start")
        stop_extractor = self.extractor(props[1], "stop")
        BaseSparkJob.__init__(self, conf, extractors=[start_extractor, stop_extractor])

    @staticmethod
    def extractor(prop, flag):
        return Extractor(
            re.compile(prop if isinstance(prop, basestring) else prop['regexp']),
            1 if isinstance(prop, basestring) or not prop.__contains__('date_index') else prop[
                'date_index'],
            2 if isinstance(prop, basestring) or not prop.__contains__('id_index') else prop[
                'id_index'],
            default_dateformat if isinstance(prop, basestring) or not prop.__contains__(
                'date_format') else prop['date_format'],
            flag)

    @staticmethod
    def update_values(new_values, previous_values):
        return new_values

    @staticmethod
    def update_state(config, values, state, update_values):

        # Inner method to check values before adding to state
        def check_values(unchecked_values):

            # Sort current values
            result_values = []
            current_flag = ''

            # Check values with start|stop order
            for value in sorted(unchecked_values):
                if value[1] != current_flag:
                    result_values += [value]
                    current_flag = value[1]
                elif current_flag == 'start':
                    del result_values[-1]
                    result_values += [value]

            # Remove first stop value if it exists
            if len(result_values) > 0 and result_values[0][1] != 'start':
                del result_values[0]

            return result_values

        from common.state import State

        # Create new state if it's none or previous one was expired
        if state is None or state.is_worked_out():
            checked_values = check_values(values)
            if len(checked_values) > 0:
                return State(update_values(checked_values, []),
                             expired_time=config.property('expire.delay'))
            else:
                return None

        # Update values for current state unexpired yet
        else:
            checked_values = check_values(state.values() + values)
            return state.update(checked_values, update_values)

    @staticmethod
    def is_valid_state(state):
        return state.is_expired() and len(state.values()) > 0

    @staticmethod
    def to_json(config, array):
        id_field = config.property('result.id_field', 'id')
        value_field = config.property('result.value_field', 'value')

        value_unit = config.property('result.value_unit', 's')
        time_functions = {
            'ns': lambda x: x * 1000000000,
            'nanosecond': lambda x: x * 1000000000,
            'mcs': lambda x: x * 1000000,
            'microsecond': lambda x: x * 1000000,
            'ms': lambda x: x * 1000,
            'millisecond': lambda x: x * 1000,
            'm': lambda x: x / 60,
            'minute': lambda x: x / 60,
            'h': lambda x: x / 3600,
            'hour': lambda x: x / 3600,
            'd': lambda x: x / 86400,
            'day': lambda x: x / 86400
        }
        time_function = time_functions[value_unit] \
            if time_functions.__contains__(value_unit) \
            else lambda x: x

        expired_time = config.property('expire.delay')
        event_time_expire = config.property('expire.event_time', True)
        expired_behavior = config.property('expire.behavior', 'fail')

        # Flag to fail package
        failed = False

        package, values = (array[0], array[1].values())
        if values[-1][1] == 'start':
            if expired_behavior == 'keep':
                from util import Utils
                values += [(Utils.datetime_add_seconds(values[-1][0], expired_time), 'stop')]
            elif expired_behavior == 'fail':
                failed = True

        start_values = [value[0] for value in values if value[1] == 'start']
        stop_values = [value[0] for value in values if value[1] == 'stop']
        durations = map(lambda (start, stop): (stop - start).total_seconds(),
                        zip(start_values, stop_values))

        if len(durations) == 0:
            return None

        json = {
            '@timestamp':
                stop_values[-1].isoformat() if len(stop_values) > 0
                else start_values[-1].isoformat(),
            value_field: time_function(sum(durations) / len(durations)),
            id_field: package
        }

        if expired_behavior == 'fail':
            json['failed'] = 'true' if failed else 'false'

        if event_time_expire and expired_behavior != 'keep':
            expired_durations = [duration for duration in durations if duration > expired_time]
            unexpired_durations = [duration for duration in durations if duration <= expired_time]

            if len(unexpired_durations) == 0:
                return None

            json[value_field] = time_function(sum(unexpired_durations) / len(unexpired_durations))
            json['failed'] = 'true' if len(expired_durations) > 0 \
                                       and expired_behavior == 'fail' else 'false'

        static_fields = config.property('result.static_fields')
        if static_fields is not None:
            json.update(static_fields)

        return json


if __name__ == "__main__":
    DurationJob().execute()
