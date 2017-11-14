from abc import ABCMeta, abstractmethod
from datetime import datetime
from dateutil.parser import parse


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
    def get_value(self, value):
        """
        Converts value for field's type
        :return: converted value
        """


class StringField(AbstractField):
    def get_value(self, value):
        """
        Converts value for field's type
        :return: string value
        """
        return value


class TimestampField(AbstractField):
    def __init__(self, name, datetime_format, output_name=None):
        AbstractField.__init__(self, name, output_name)
        self.__datetime_format = datetime_format

    def get_value(self, value):
        """
        Converts value for field's type
        :return: datetime value
        :raises: ParsingException for wrong date format
        """
        try:
            return datetime.strptime(value, self.__datetime_format)
        except ValueError:
            raise ParsingException("date parsing error")


class TimestampFieldWithTimeZone(AbstractField):
    def __init__(self, name, output_name=None):
        AbstractField.__init__(self, name, output_name)

    def get_value(self, value):
        """
        Converts value for field's type
        :return: datetime value
        :raises: ParsingException for wrong date format
        """
        try:
            return parse(value, fuzzy=True)
        except ValueError:
            raise ParsingException("wrong datetime format")

class IntField(AbstractField):
    def get_value(self, value):
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
    def get_value(self, value):
        """
        Converts value for field's type
        :return: float value
        :raises: ParsingException for not float value
        """
        try:
            return float(value)
        except ValueError:
            raise ParsingException("float parsing error")
