from applications.basic_analytics.vrops.vm_datastore_disk_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase

class VropsVMDatastoreDiskBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    def test_events_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/vrops/vm_datastore_disk_stats/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/vrops/vm_datastore_disk_stats/input",
            expected_result_file="test/it/resources/basic_analytics/vrops/vm_datastore_disk_stats/expected_result.txt"
        )
