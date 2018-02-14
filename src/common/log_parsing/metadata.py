from abc import ABCMeta, abstractmethod
from datetime import datetime
from dateutil.parser import parse

CONTEXT_TIMEZONE = "timezone"


class Metadata:
    """
    Metadata class which consists of field's types
    """

    def __init__(self, fields):
        """
        :param fields: field types array. Any AbstractField inheritor is allowed.
        """
        self.__fields = fields
        self.__name_to_fields_dict = self.__create_name_to_fields_dict(fields)

    @staticmethod
    def __create_name_to_fields_dict(fields):
        return {field.get_name(): field for field in fields}

    def get_fields_amount(self):
        """
        Returns fields amount
        :return: fields amount
        """
        return len(self.__fields)

    def get_field_by_idex(self, index):
        """
        Returns field's metadata by index
        :param index: field's index
        :return: field's metadata
        """
        return self.__fields[index]

    def get_field_by_name(self, name):
        """
        Returns field's metadata by name
        :param name: field's name
        :return: field's metadata
        """
        return self.__name_to_fields_dict.get(name)


class ParsingException(Exception):
    """
    Base exception for log parsing. This exception is a reason to send message to DLQ
    """
    pass


class AbstractField:
    """
    Abstract field class
    """
    __metaclass__ = ABCMeta

    def __init__(self, name, output_name=None):
        """
        :param name: field's name
        :param output_name: field's output name
        """
        self.__name = name
        self.__output_name = output_name

    def get_name(self):
        """
        Returns field's name
        :return: field's name
        """
        return self.__name

    def get_output_name(self):
        """
        Returns field's output name
        :return: field's output name
        """
        return self.__output_name if self.__output_name is not None else self.__name

    @abstractmethod
    def get_value(self, value, context):
        """
        Converts value of string into value of field's type
        :param value value of string type to be converted
        :param context dictionary provided by EvenCreator which can contain useful values related to event being parsed.
        :return: converted value
        """


class StringField(AbstractField):
    def get_value(self, value, context=None):
        """
        Converts value for field's type
        :return: string value
        """
        return value


class TimestampField(AbstractField):
    """
    This class doesn't take into account timezone of a field. Consider using of ConfigurableTimestampField.
    """

    def __init__(self, name, datetime_format, output_name=None):
        AbstractField.__init__(self, name, output_name)
        self.__datetime_format = datetime_format

    def get_value(self, value, context=None):
        """
        Converts value for field's type
        :return: datetime value
        :raises: ParsingException for wrong date format
        """
        try:
            return datetime.strptime(value, self.__datetime_format)
        except ValueError:
            raise ParsingException("date parsing error")


class IntField(AbstractField):
    def get_value(self, value, context=None):
        """
        Converts value for field's type
        :return: int value
        :raises: ParsingException for not int value
        """
        try:
            return int(value)
        except ValueError:
            raise ParsingException("int parsing error")


class FloatField(AbstractField):
    def get_value(self, value, context=None):
        """
        Converts value for field's type
        :return: float value
        :raises: ParsingException for not float value
        """
        try:
            return float(value)
        except ValueError:
            raise ParsingException("float parsing error")


class AbstractEventCreator:
    __metaclass__ = ABCMeta

    def __init__(self, metadata, parser, matcher=None, timezone_field="tz"):
        """
        Creates event creator for list parser
        :param metadata: metadata
        :param parser: list parser
        :param matcher: matcher object to check the input line to perform the parsing step only if the line is matched
        :param timezone_field: field name with information about timezone
        """
        self._metadata = metadata
        self._parser = parser
        self._matcher = matcher
        self._timezone_field = timezone_field

    def _create_context(self, row):
        """
        Creates dictionary with timezone. That dictionary is referred as a parsing context and can be extended
        if other data is needed in field values.
        :param row: incoming row.
        :return: dictionary with context data for parsing event. Currently it contains
            - 'timezone' value taken from input json.
        """
        context = {}
        if self._timezone_field in row:
            context[CONTEXT_TIMEZONE] = row[self._timezone_field]
        return context

    def create(self, row):
        """
        Creates event for given row
        :param row: input row
        :return: dict with result fields
        :raises: ParsingException when values amount isn't equal metadata fields amount
        """
        context = self._create_context(row)
        return self._create_with_context(row, context)

    @abstractmethod
    def _create_with_context(self, row, context):
        """

        :param row:
        :param context:
        :return:
        """
