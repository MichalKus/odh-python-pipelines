import re


class Transformer:
    """
    Utility class that may transform text by applying pattern with replacement, trimming and optionally converting to lower case
    """
    def __init__(self, pattern, replacement, to_lower_case=True):
        self.__to_lower_case = to_lower_case
        self.__pattern = pattern
        self.__replacement = replacement

    def transform(self, text):
        transformed = re.sub(self.__pattern, self.__replacement, text.strip())
        if self.__to_lower_case:
            return transformed.lower()
        return transformed
