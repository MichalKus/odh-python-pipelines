import unittest


class BaseMultipleMessageParsingTestCase(unittest.TestCase):
    event_creators = None

    def assert_parsing(self, row, event):
        self.maxDiff = None
        return self.assertEqual(
            self.event_creators.get_parsing_context(row).event_creator.create(row),
            event
        )
