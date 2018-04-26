from applications.basic_analytics.stb_general.stb_live_viewing_report_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbLiveViewingReportBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/" +
                               "stb_general/stb_live_viewing_report/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_general/stb_live_viewing_report/input",
            expected_result_file="test/it/resources/basic_analytics/" +
                                 "stb_general/stb_live_viewing_report/expected_result.txt"
        )
