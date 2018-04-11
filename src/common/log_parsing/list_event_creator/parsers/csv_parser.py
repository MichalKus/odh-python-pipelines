"""Module for csv parser"""
import csv
from StringIO import StringIO


class CsvParser(object):
    """
    Parses event using csv format
    """

    def __init__(self, delimiter=',', quotechar='"', skipinitialspace=False):
        """
        Creates instances for split parsing
        :param delimiter: delimiter
        """
        self._delimiter = delimiter
        self._quotechar = quotechar
        self._skipinitialspace = skipinitialspace

    def parse(self, text):
        """
        Parses text using csv parser
        :param text: input text
        :return: parsed list
        """
        row_as_file = StringIO(text)
        csv_reader = csv.reader(row_as_file, delimiter=self._delimiter, quotechar=self._quotechar,
                                skipinitialspace=self._skipinitialspace)
        for parsed_list in csv_reader:
            return parsed_list
