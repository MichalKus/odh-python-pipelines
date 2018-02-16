from applications.adv_analytics.cloudmap_vm_correlation.driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase

class VropsCloudmapVMCorrelationTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/adv_analytics/vrops/cloudmap_vm_correlation/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/adv_analytics/vrops/cloudmap_vm_correlation/input",
            expected_result_file="test/it/resources/adv_analytics/vrops/cloudmap_vm_correlation/expected_result.txt"
        )
