from applications.basic_analytics.eos_stb_reports.usage_collector_event_type_report_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase

class UsageCollectorEventTypeReportBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/eos_stb_reports/usage_collector_event_type_report/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/eos_stb_reports/usage_collector_event_type_report/input",
            expected_result_file="test/it/resources/basic_analytics/eos_stb_reports/usage_collector_event_type_report/expected_result.txt"
        )
