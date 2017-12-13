from os import listdir
from os.path import isfile, join
from dateutil.parser import parse
from dateutil.tz import tzfile
from metadata import AbstractField, ParsingException
from metadata import CONTEXT_TIMEZONE


def _get_available_timezones(path, region=""):
    """
    It scans directory specified by path for timezone files and returns map of timezone names to file paths.
    :param path: root path for search
    :param region: region used as prefix for timezone name. If you search from root leave region empty or default.
    :return: map of timezone names to filepath with related data
    """
    result = []
    for filename in listdir(path):
        filepath = join(path, filename)
        if isfile(filepath):
            result.append((region + filename, filepath))
        else:
            result.extend(_get_available_timezones(filepath, region + filename + "/"))
    return result


def load_timezones(path):
    """
    Loading of nevessary timezones/
    :param path: root path to start search for timezone files from
    :return: map of timezone names to tzinfo objects
    """
    timezone_map = {}
    for timezone_file_name in _get_available_timezones(path):
        timezone_map[timezone_file_name[0]] = tzfile(timezone_file_name[1])
    return timezone_map


class ConfigurableTimestampField(AbstractField):
    """
    This timestamp field describes datetime field in metadata and takes timezone from 3 possible sources:
        - constructor parameter (referred as a default value - d)
        - value being parsed (referred as inherent value - i)
        - context["timezone"] (referred as context value - c)
    Priorities of mentioned sources are specified as a constructor parameter.
    """

    def __init__(self, name, default_timezone_name, priorities="dic", output_name=None, dayfirst=False,
                 yearfirst=False):
        """
        Constructor.
        :param name: field name
        :param default_timezone_name: timezone name used as default
        :param priorities: possible values are "dic", "dci", "cdi", "cid", "idc", "icd"
        :param output_name: a name used in output events
        """
        AbstractField.__init__(self, name, output_name)
        self._default_timezone_name = default_timezone_name
        self.dayfirst = dayfirst
        self.yearfirst = yearfirst
        self._timezone_functions = self.__get_prioritized_timezone_function(priorities)

    def __set_default_timezone_date(self, date, context):
        """
        Sets default timezone in datetime object.
        :param date: datetime object to set timezone
        :param context: dictionary with additional data. It's not used in the method.
        :return: datetime object with default timezone. If default timezone is not defined None is returned.
        """
        try:
            timezone = timezones[self._default_timezone_name]
            return date.replace(tzinfo=timezone)
        except:
            return None

    def __set_inherent_timezone_date(self, date, context):
        """
        Leaves timezone of a datetime object unchanged if it is defined withing the datetime object.
        :param date: datetime object to set timezone
        :param context: dictionary with additional data. It's not used in the method.
        :return: datetime object if it has timezone defined or None
        """
        return None if (date.tzinfo is None) else date

    def __set_context_timezone_date(self, date, context):
        """
        Sets timezone from context.
        :param date: datetime object to set timezone
        :param context: dictionary with metadata.CONTEXT_TIMEZONE set to desired timezone.
        :return: datetime object with defined timezone or None if context empty of timezone name can't be resolved.
        """
        try:
            timezone = timezones[context[CONTEXT_TIMEZONE]]
            return date.replace(tzinfo=timezone)
        except:
            return None

    def __get_prioritized_timezone_function(self, priorities):
        """
        Constract an array of "set timezone" function in priority order
        :param priorities: possible values are "dic", "dci", "cdi", "cid", "idc", "icd"
        :return: array of functions applying timezones to datetime object.
        """
        priority_map = {"d": self.__set_default_timezone_date,
                        "i":  self.__set_inherent_timezone_date,
                        "c": self.__set_context_timezone_date}
        return [priority_map[item] for item in priorities]

    def __apply_timezone(self, date, context):
        """
        Updating time zone according to specified priorities.
        :param date: datetime object for timezone to be applied.
        :param context: dictionary with additional data.
        :return: datetime object with defined timezone.
        :exception ParsingException if no timezone is found.
        """
        for timezone_function in self._timezone_functions:
            date_with_timezone = timezone_function(date, context)
            if date_with_timezone is not None:
                return date_with_timezone
        raise ParsingException("Time zone is missing in input data, default configuration and timezone field.")

    def get_value(self, value, context=None):
        """
        Converts value for field's type
        :return: datetime value
        :raises: ParsingException if date format is wrong or no timezone is found
        """
        try:
            return self.__apply_timezone(
                parse(value, fuzzy=True, dayfirst=self.dayfirst, yearfirst=self.yearfirst), context)
        except ValueError:
            raise ParsingException("wrong datetime format")


"""
Load timezones once
"""
if "timezones" not in globals():
    timezones = load_timezones("resources/timezones")
