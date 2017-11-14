import re
from util.utils import Utils


class VsppUDF(object):
    megabyte = float(1024 * 1024)
    __startRegex = re.compile("\[([^\]]*)\].*?INFO - Fabrix details")
    __endRegex = re.compile("\[([^\]]*)\].*?INFO - Fabrix progress.*?'success_full_ingest'\s*:\s*'1'")
    __sizeRegex = re.compile("'total_size'\s*:\s*'(\d*)'")

    @staticmethod
    def process(text):
        try:
            lines = filter(lambda text_line: text_line != "", text.split('\n'))
            durations = []
            for line in lines:
                startMatch = VsppUDF.__startRegex.match(line)
                if startMatch:
                    start = Utils.parse_datetime(startMatch.group(1))
                endMatch = VsppUDF.__endRegex.match(line)
                if endMatch:
                    delta = Utils.parse_datetime(endMatch.group(1)) - start
                    durations.append((start, delta.seconds))
            sizes = [int(size) for size in re.findall(VsppUDF.__sizeRegex, line)]
            return map(lambda ((eventTime, duration), size): (eventTime, size / (duration * VsppUDF.megabyte)), zip(durations, sizes))
        except ZeroDivisionError:
            return []

