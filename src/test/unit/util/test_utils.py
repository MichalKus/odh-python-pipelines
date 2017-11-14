import unittest
from datetime import datetime
from util.utils import Utils, default_date_format


class UtilsTestCase(unittest.TestCase):
    def test_parse(self):
        self.assertEquals(datetime.strptime("2017-07-04 09:17:32,210", default_date_format),
                          Utils.parse_datetime("2017-07-04 09:17:32,210"))

    def test_load_file(self):
        self.assertTrue(len(Utils.load_file("test/unit/resources/vspp.log")) > 0)


if __name__ == '__main__':
    unittest.main()
