from common.log_parsing.metadata import AbstractField, ParsingException

class CustomDictField(AbstractField):
    def get_value(self, value, context=None):
        """
        Converts value (influx string) for field's type
        :return: dict value
        :raises: ParsingException for not int value
        """
        try:
            pairs = []
            for x in value.split(','):
                key = x.split('=')[0]
                try:
                    value = float(x.split('=')[1])
                except ValueError:
                    value = x.split('=')[1]
                pairs.append((key,value))
            return dict(pairs)
        except IndexError:
            raise ParsingException("dict parsing error")
