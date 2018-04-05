"""Module for utility classes related to configuration"""
from abc import ABCMeta, abstractmethod

from common.log_parsing.metadata import ParsingException


class MultisourceConfiguration(object):
    """
    Interface for all nodes in configuration tree
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_parsing_context(self, row):
        """abstract create pipeline method"""


class MatchField(MultisourceConfiguration):
    """
    Node of tree, contains field name for matching and map with leaves
    """
    def __init__(self, field_name='root', children_map=None, direct_match=False):
        """
        Creates node of configuration tree
        :param field_name: Field name for matching
        :param children_map: Map with different patterns for matching with field_name and children nodes
        :param direct_match: Boolean setting for full matching or not
        """
        self.__field_name = field_name
        self.__direct_match = direct_match
        self.__children_map = children_map

    def add_child(self, children_node):
        """
        Add new pattern for field_name
        :param children_node: Additional pattern for existing field_name
        """
        for key, value in children_node:
            assert isinstance(value, MultisourceConfiguration)
            self.__children_map.update(children_node)

    def get_parsing_context(self, row):
        """
        Choose method for matching depends on parameter direct_match
        :param row: Row from kafka topic
        :return: Next child node
        """
        if self.__direct_match:
            return self.__get_exact_match(row)
        else:
            return self.__get_pattern_match(row)

    def __get_exact_match(self, row):
        """
        Getting next child mode with key = row[field]
        :param row: Row from kafka topic
        :return: Next child node of tree with direct match of key
        """
        if row[self.__field_name] in self.__children_map:
            return self.__children_map[row[self.__field_name]]
        else:
            raise ParsingException("Event creator wasn't found")

    def __get_pattern_match(self, row):
        """
        Getting next child mode with key in row[field]
        :param row: Row from kafka topic
        :return: Next child node of tree with pattern match of key
        """
        for key, value in self.__children_map.iteritems():
            if key in row[self.__field_name]:
                return value.get_parsing_context(row)
        raise ParsingException("Event creator wasn't found")


class SourceConfiguration(MultisourceConfiguration):
    """
    Leaf of tree that contains of event_creator for Row
    """
    def __init__(self, event_creator, output_topic):
        self.event_creator = event_creator
        self.output_topic = output_topic

    def get_parsing_context(self, row):
        """
        Return leaf with event_creator and output_topic
        :param row: Row from kafka topic
        :return: Tuple of EventCreator and OutputTopic
        """
        return self
