from applications.basic_analytics.stb_tuner_report.driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class StbTunerReportBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/stb_tuner_report/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/stb_tuner_report/input",
            expected_result_file="test/it/resources/basic_analytics/stb_tuner_report/expected_result.txt"
        )
