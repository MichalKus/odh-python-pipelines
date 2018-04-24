"""Module for counting all general analytics metrics for EOS STB CPE SettingsReport"""
from pyspark.sql.types import StructField, StructType, TimestampType, StringType, ArrayType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, explode


class CpeSettingsReportEventProcessor(BasicAnalyticsProcessor):
    """Class that's responsible to create pipelines for CPE Settings Reports"""

    def _process_pipeline(self, read_stream):

        self._read_stream = read_stream
        self._common_pipeline = read_stream \
            .select("@timestamp",
                    "SettingsReport.*",
                    "header.*")
        # Common CPESettings Report
        self._common_settings_pipeline = self._common_pipeline \
            .select("@timestamp",
                    "settings.*",
                    col("viewerID").alias("viewer_id"))

        return [self.distinct_total_cpe_settings_report_count(),
                self.distinct_total_cpe_with_hdmi_cec_active(),
                self.distinct_total_cpe_with_suspend_status(),
                self.distinct_total_cpe_with_audio_dolby_digital(),
                self.distinct_total_cpe_with_personalized_suggestions(),
                self.distinct_total_cpe_with_audio_dolby_digitalaccepted_app_user_agreement(),
                self.distinct_total_cpe_with_auto_subtitles_enabled(),
                self.distinct_total_cpe_with_auto_subtitles_disabled(),
                self.distinct_total_cpe_with_active_standby(),
                self.distinct_total_cpe_with_cold_standby(),
                self.distinct_cpe_with_lukewarm_standby(),
                self.distinct_cpe_count_with_upgrade_status(),
                self.distinct_total_cpe_count_with_cpe_country(),
                self.distinct_cpe_count_recently_used_settings_items(),
                self.distinct_cpe_with_age_restriction_enabled(),
                self.distinct_cpe_with_selected_audio_track_language(),
                self.distinct_cpe_with_selected_subtitles_track_language(),
                self.distinct_cpe_factory_reset_report(),
                self.distinct_tv_brands_paired_with_each_cpe(),
                self.distinct_audio_brands_paired_with_each_cpe()]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("SettingsReport", StructType([
                StructField("type", StringType()),
                StructField("settings", StructType([
                    StructField("cpe.enableCEC", StringType()),
                    StructField("customer.isSuspended", StringType()),
                    StructField("cpe.audioDolbyDigital", StringType()),
                    StructField("customer.personalSuggestions", StringType()),
                    StructField("customer.appsOptIn", StringType()),
                    StructField("profile.subControl", StringType()),
                    StructField("cpe.standByMode", StringType()),
                    StructField("cpe.country", StringType()),
                    StructField("cpe.upgradeStatus", StringType()),
                    StructField("profile.recentlyUsedSettingsItems", ArrayType(StringType())),
                    StructField("profile.ageLock", StringType()),
                    StructField("profile.audioLang", StringType()),
                    StructField("profile.subLang", StringType()),
                    StructField("cpe.factoryResetState", StringType()),
                    StructField("cpe.quicksetPairedDevicesInfo", StructType([
                        StructField("amp", StructType([
                            StructField("isPaired", StringType()),
                            StructField("brand", StringType())
                        ])),
                        StructField("tv", StructType([
                            StructField("brand", StringType())
                        ]))
                    ]))
                ]))
            ])),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
        ])

    # Total CPE Reporting Settings Data
    def distinct_total_cpe_settings_report_count(self):
        return self._common_pipeline \
            .select("@timestamp", "type", col("viewerID").alias("viewer_id")) \
            .filter("type == 'SettingsReport'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".SettingsReport"))

    # Total CPE with HDMI-CEC Active
    def distinct_total_cpe_with_hdmi_cec_active(self):
        return self._common_settings_pipeline \
            .filter(col("`cpe.enableCEC`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".enable_cec"))

    # Total CPE with suspended Status
    def distinct_total_cpe_with_suspend_status(self):
        return self._common_settings_pipeline \
            .filter(col("`customer.isSuspended`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".is_suspended"))

    # Total CPE with Digital Dolby Audio
    def distinct_total_cpe_with_audio_dolby_digital(self):
        return self._common_settings_pipeline \
            .filter(col("`cpe.audioDolbyDigital`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".audio_dolby_digital"))

    # Total CPE with personalised suggestions
    def distinct_total_cpe_with_personalized_suggestions(self):
        return self._common_settings_pipeline \
            .filter(col("`customer.personalSuggestions`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".personalized_suggestions"))

    # CPE Accepted App User Agreement:
    def distinct_total_cpe_with_audio_dolby_digitalaccepted_app_user_agreement(self):
        return self._common_settings_pipeline \
            .filter(col("`customer.appsOptIn`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".accepted_app_user_agreement"))

    # CPE with auto Subtitles enabled
    def distinct_total_cpe_with_auto_subtitles_enabled(self):
        return self._common_settings_pipeline \
            .filter(col("`profile.subControl`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".subtitles_enabled"))

    # CPE with auto Subtitles disabled
    def distinct_total_cpe_with_auto_subtitles_disabled(self):
        return self._common_settings_pipeline \
            .filter(col("`profile.subControl`") == 'false') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".subtitles_disabled"))

    # CPE Reporting ActiveStandby
    def distinct_total_cpe_with_active_standby(self):
        return self._common_settings_pipeline \
            .filter(col("`cpe.standByMode`") == 'ActiveStandby') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".active_standby"))

    # CPE Reporting ColdStandby
    def distinct_total_cpe_with_cold_standby(self):
        return self._common_settings_pipeline \
            .filter(col("`cpe.standByMode`") == 'ColdStandby') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cold_standby"))

    # CPE Reporting LukewarmStandby
    def distinct_cpe_with_lukewarm_standby(self):
        return self._common_settings_pipeline \
            .filter(col("`cpe.standByMode`") == 'LukewarmStandby') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".lukewarm_standby"))

    # CPE report based on the upgrade status
    def distinct_cpe_count_with_upgrade_status(self):
        return self._common_settings_pipeline \
            .select("@timestamp",
                    "viewer_id",
                    col("`cpe.country`").alias("cpe_country"),
                    col("`cpe.upgradeStatus`").alias("upgrade_status")) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["cpe_country", "upgrade_status"],
                                     aggregation_name=self._component_name))

    # CPE report based on the upgrade status
    def distinct_total_cpe_count_with_cpe_country(self):
        return self._common_settings_pipeline \
            .select("@timestamp",
                    "viewer_id",
                    col("`cpe.country`").alias("cpe_country")) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["cpe_country"],
                                     aggregation_name=self._component_name))

    # CPE Report on Recent used Menu's
    def distinct_cpe_count_recently_used_settings_items(self):
        return self._common_settings_pipeline \
            .select("@timestamp",
                    "viewer_id",
                    explode("`profile.recentlyUsedSettingsItems`").alias("settings_items")) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     group_fields=["settings_items"],
                                     aggregation_name=self._component_name))

    # CPE count with Age restriction enabled/disabled'
    def distinct_cpe_with_age_restriction_enabled(self):
        return self._common_settings_pipeline \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["`profile.ageLock`"],
                                     aggregation_name=self._component_name))

    # CPE with selected audio track Language
    def distinct_cpe_with_selected_audio_track_language(self):
        return self._common_settings_pipeline \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["`profile.audioLang`"],
                                     aggregation_name=self._component_name))

    # CPE with selected Subtitles track Language
    def distinct_cpe_with_selected_subtitles_track_language(self):
        return self._common_settings_pipeline \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["`profile.subLang`"],
                                     aggregation_name=self._component_name))

    # CPE Factory Reset Report
    def distinct_cpe_factory_reset_report(self):
        return self._common_settings_pipeline \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["`cpe.factoryResetState`"],
                                     aggregation_name=self._component_name))

    # TV Brands Paired with each CPE'
    def distinct_tv_brands_paired_with_each_cpe(self):
        return self._common_settings_pipeline \
            .select("@timestamp",
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("tv").getItem("brand").alias("brand"),
                    "viewer_id") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["brand"],
                                     aggregation_name=self._component_name))

    # Audio Brands Paired with each CPE'
    def distinct_audio_brands_paired_with_each_cpe(self):
        return self._common_settings_pipeline \
            .select("@timestamp",
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("amp").getItem("brand").alias("brand"),
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("amp").getItem("isPaired").alias("is_paired"),
                    "viewer_id") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["is_paired", "brand"],
                                     aggregation_name=self._component_name))


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return CpeSettingsReportEventProcessor(configuration, CpeSettingsReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
