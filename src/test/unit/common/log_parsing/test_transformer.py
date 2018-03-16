import unittest

from common.log_parsing.transformer import Transformer


class EventCreatorTestCase(unittest.TestCase):

    def test_transform(self):
        self.assertEquals(Transformer('(?<!^)(?=[A-Z])', '_').transform("AnyMessage"), "any_message")


if __name__ == '__main__':
    unittest.main()
