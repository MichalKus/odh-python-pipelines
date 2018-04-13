"""Module for event creator that can parse url in message"""
import urlparse

from common.spark_utils.custom_functions import convert_to_underlined


class EventWithUrlCreator(object):
    """
    Extended EventCreator with parsing URL inside message
    """

    def __init__(self, url_field="url", url_query_field=None, delete_source_field=False, keys_to_underscore=True):
        """
        Creates event creator with parsing URL or only url query
        :param url_field: field that contains full url with or without parameters
        :param url_query_field: field that contains only url query
        """
        self._url_field = url_field
        self._url_query_field = url_query_field
        self._delete_source_field = delete_source_field
        self._keys_to_underscore = keys_to_underscore

    def create(self, row):
        """
        Parse row with Parser and then parse URL
        :param row: Row from kafka topic
        :return: list of all fields with all URL parameters
        """
        return self.split_url(row)

    def split_url(self, values):
        """
        Find field url and parse it
        :param values: List of fields after first split
        :return: list of all fields with all URL parameters
        """
        if self._url_query_field:
            params = dict(urlparse.parse_qsl(values[self._url_query_field]))
            if self._delete_source_field:
                del values[self._url_query_field]
            return dict([(convert_to_underlined(x[0]), x[1]) for x in params.items()]) \
                if self._keys_to_underscore else params

        url = values[self._url_field].split("?")
        if self._delete_source_field:
            del values[self._url_field]
        if len(url) >= 2:
            all_parameters = url[1]
            params = dict(urlparse.parse_qsl(all_parameters))
            if self._keys_to_underscore:
                params = dict([(convert_to_underlined(x[0]), x[1]) for x in params.items()])
            params.update({"action": url[0]})
            return params
        else:
            return {"action": url[0]}

    def contains_fields_to_parse(self, row):
        return self._url_field in row.keys() or self._url_query_field in row.keys()
