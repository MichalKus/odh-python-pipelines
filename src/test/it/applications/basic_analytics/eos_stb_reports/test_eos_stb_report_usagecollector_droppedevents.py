from applications.basic_analytics.eos_stb.general_driver import GeneralEosStbProcessor
from applications.basic_analytics.eos_stb_reports.error_level_report_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase

class DroppedEventsReportBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/usagecollector_droppedevents/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/usagecollector_droppedevents/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/usagecollector_droppedevents/expected_result.txt"
        )
