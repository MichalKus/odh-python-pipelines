import unittest
from util.utils import Utils
from applications.ingest_vspp.vspp_udf import VsppUDF


class VsppUdfTestCase(unittest.TestCase):
    __udf = VsppUDF()

    def test_event_time(self):
        text = Utils.load_file("test/unit/resources/vspp.log")

        self.assertEquals(Utils.parse_datetime('2017-06-21 13:47:00,480'), self.__udf.process(text)[0][0])
        self.assertEquals(Utils.parse_datetime('2017-06-21 13:47:50,606'), self.__udf.process(text)[1][0])

    def test_sizes(self):
        text = Utils.load_file("test/unit/resources/vspp.log")
        self.assertEquals(3.62421875, self.__udf.process(text)[0][1])
        self.assertEquals(1.37421875, self.__udf.process(text)[1][1])
        
    def test_zero_division_error(self):
        text = Utils.load_file("test/unit/resources/vspp_error.log")
        self.assertTrue(len(self.__udf.process(text)) == 0)

if __name__ == '__main__':
    unittest.main()
