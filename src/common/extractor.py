from util import Utils


class Extractor(object):
    def __init__(self, regexp, date_index=1, unique_index=2,
                 date_format="%Y-%m-%d %H:%M:%S,%f", flag=None):
        self.__regexp = regexp
        self.__date_index = date_index
        self.__unique_index = unique_index
        self.__date_format = date_format
        self.__flag = flag

    def extract(self, text):
        match = self.__regexp.match(text)
        if match:
            date = Utils.parse_datetime(match.group(self.__date_index), self.__date_format)
            package = match.group(self.__unique_index)
            return (package, date) if self.__flag is None else (package, (date, self.__flag))
        else:
            return None
