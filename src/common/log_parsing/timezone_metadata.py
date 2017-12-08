from os import listdir
from os.path import isfile, join
from dateutil.parser import parse
from dateutil.tz import tzoffset, tzfile
from metadata import AbstractField, ParsingException
from metadata import CONTEXT_TIMEZONE


def get_available_timezones(path, region=""):
    result = []
    for filename in listdir(path):
        filepath = join(path, filename)
        if isfile(filepath):
            result.append((region + filename, filepath))
        else:
            result.extend(get_available_timezones(filepath, region + filename + "/"))
    return result


def load_timezones(path):
    timezone_map = {}
    for timezone_file_name in get_available_timezones(path):
        timezone_map[timezone_file_name[0]] = tzfile(timezone_file_name[1])
    return timezone_map


class ConfigurableTimestampField(AbstractField):
    """
    This timestamp field takes data from message and timezone from 3 possible sources:
        - constructor parameter (referred as a default value - d)
        - value being parsed (referred as inherent value - i)
        - context["timezone"] (referred as context value - c)
    Priorities of mentioned sources are specified as a constructor parameter.
    """

    def __init__(self, name, timezone_name, priorities="dic", output_name=None, dayfirst=True, yearfirst=False):
        """

        :param name:
        :param timezone_name:
        :param priorities: possible values are "dic", "dci", "cdi", "cid", "idc", "icd"
        :param output_name:
        """
        AbstractField.__init__(self, name, output_name)
        self._timezone_name = timezone_name
        self.dayfirst = dayfirst
        self.yearfirst = yearfirst
        self._timezone_functions = self._get_prioritized_timezone_function(priorities)

    def _get_default_timezone_date(self, date, context):
        try:
            timezone = timezones[self._timezone_name]
            return date.replace(tzinfo=timezone)
        except:
            return None

    def _get_inherent_timezone_date(self, date, context):
        if date.tzinfo is None:
            return None
        else:
            return date

    def _get_context_timezone_date(self, date, context):
        try:
            timezone = timezones[context[CONTEXT_TIMEZONE]]
            return date.replace(tzinfo=timezone)
        except:
            return None

    def get_timezone_by_name(self, timezone_name):
        pass

    def _get_prioritized_timezone_function(self, priorities):
        priority_map = {"d": self._get_default_timezone_date,
                        "i":  self._get_inherent_timezone_date,
                        "c": self._get_context_timezone_date}
        return [priority_map[item] for item in priorities]

    def _apply_timezone(self, date, context):
        """
        Updating time zone according to specified priorities.
        :param date:
        :param context:
        :return:
        """
        for timezone_function in self._timezone_functions:
            date_with_timezone = timezone_function(date, context)
            if date_with_timezone is not None:
                return date_with_timezone
        return date

    def get_value(self, value, context=None):
        """
        Converts value for field's type
        :return: datetime value
        :raises: ParsingException for wrong date format
        """
        try:
            return self._apply_timezone(parse(value, fuzzy=True, dayfirst=self.dayfirst, yearfirst=self.yearfirst), context)
        except ValueError:
            raise ParsingException("wrong datetime format")


if "timezones" not in globals():
    timezones = load_timezones("resources/timezones")
