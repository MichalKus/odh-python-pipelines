from applications.basic_analytics.stb_general.vm_stat_report_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbVmStatAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_100_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb_general/vm_stat/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_general/vm_stat/input/",
            expected_result_file="test/it/resources/basic_analytics/stb_general/vm_stat/expected_result.txt"
        )
