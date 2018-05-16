"""
Module for counting all general analytics metrics for EOS STB CPE SettingsReport
"""
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, LongType

from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from common.spark_utils.custom_functions import convert_epoch_to_iso
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from common.basic_analytics.aggregations import DistinctCount
from pyspark.sql.functions import col, explode, when


class CpeSettingsReportEventProcessor(BasicAnalyticsProcessor):
    """
    Class that's responsible to create pipelines for CPE Settings Reports
    """

    def _prepare_timefield(self, data_stream):
        return convert_epoch_to_iso(data_stream, "SettingsReport.ts", "@timestamp")

    def _process_pipeline(self, read_stream):
        common_pipeline = read_stream \
            .select("@timestamp",
                    "SettingsReport.*",
                    "header.*")

        common_settings_pipeline = common_pipeline \
            .select("@timestamp",
                    "settings.*",
                    col("viewerID").alias("viewer_id"))

        return [self.distinct_total_cpe_settings_report_count(common_pipeline),
                self.distinct_total_cpe_with_hdmi_cec_active(common_settings_pipeline),
                self.distinct_total_cpe_with_suspend_status(common_settings_pipeline),
                self.distinct_total_cpe_with_audio_dolby_digital(common_settings_pipeline),
                self.distinct_total_cpe_with_personalized_suggestions(common_settings_pipeline),
                self.distinct_total_cpe_with_audio_dolby_digital_accepted_app_user_agreement(common_settings_pipeline),
                self.distinct_total_cpe_with_audio_dolby_digital_not_accepted_app_user_agreement(
                    common_settings_pipeline),
                self.distinct_total_cpe_with_auto_subtitles_enabled(common_settings_pipeline),
                self.distinct_total_cpe_with_auto_subtitles_disabled(common_settings_pipeline),
                self.distinct_total_cpe_with_active_standby(common_settings_pipeline),
                self.distinct_total_cpe_with_cold_standby(common_settings_pipeline),
                self.distinct_cpe_with_lukewarm_standby(common_settings_pipeline),
                self.distinct_cpe_count_with_upgrade_status(common_settings_pipeline),
                self.distinct_cpe_count_recently_used_settings_items(common_settings_pipeline),
                self.distinct_cpe_with_age_restriction_enabled(common_settings_pipeline),
                self.distinct_cpe_with_selected_audio_track_language(common_settings_pipeline),
                self.distinct_cpe_with_selected_subtitles_track_language(common_settings_pipeline),
                self.distinct_cpe_factory_reset_report(common_settings_pipeline),
                self.distinct_tv_brands_paired_with_each_cpe(common_settings_pipeline),
                self.distinct_audio_brands_paired_with_each_cpe(common_settings_pipeline)]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("SettingsReport", StructType([
                StructField("type", StringType()),
                StructField("ts", LongType()),
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
                            StructField("isPaired", StringType()),
                            StructField("brand", StringType())
                        ]))
                    ]))
                ]))
            ])),
            StructField("header", StructType([
                StructField("viewerID", StringType())
            ])),
        ])

    def distinct_total_cpe_settings_report_count(self, common_pipeline):
        return common_pipeline \
            .select("@timestamp", "type", col("viewerID").alias("viewer_id")) \
            .filter("type == 'SettingsReport'") \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpeReporting_settings_data"))

    def distinct_total_cpe_with_hdmi_cec_active(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`cpe.enableCEC`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_hdmi_cec_active"))

    def distinct_total_cpe_with_suspend_status(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`customer.isSuspended`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_suspended_status"))

    def distinct_total_cpe_with_audio_dolby_digital(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`cpe.audioDolbyDigital`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_audio_dolby_digital"))

    def distinct_total_cpe_with_personalized_suggestions(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`customer.personalSuggestions`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_personalized_suggestions"))

    def distinct_total_cpe_with_audio_dolby_digital_accepted_app_user_agreement(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`customer.appsOptIn`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_accepted_app_user_agreement"))

    def distinct_total_cpe_with_audio_dolby_digital_not_accepted_app_user_agreement(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`customer.appsOptIn`") == 'false') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name +
                                     ".cpe_with_not_accepted_app_user_agreement"))

    def distinct_total_cpe_with_auto_subtitles_enabled(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`profile.subControl`") == 'true') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_subtitles_enabled"))

    def distinct_total_cpe_with_auto_subtitles_disabled(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`profile.subControl`") == 'false') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_with_subtitles_disabled"))

    def distinct_total_cpe_with_active_standby(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`cpe.standByMode`") == 'ActiveStandby') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_reporting_active_standby"))

    def distinct_total_cpe_with_cold_standby(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`cpe.standByMode`") == 'ColdStandby') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_reporting_cold_standby"))

    def distinct_cpe_with_lukewarm_standby(self, common_settings_pipeline):
        return common_settings_pipeline \
            .filter(col("`cpe.standByMode`") == 'LukewarmStandby') \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     aggregation_name=self._component_name + ".cpe_reporting_lukewarm_standby"))

    def distinct_cpe_count_with_upgrade_status(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    "viewer_id",
                    col("`cpe.country`").alias("cpe_country"),
                    col("`cpe.upgradeStatus`").alias("upgrade_status")) \
            .where("cpe_country is not NULL") \
            .where("upgrade_status is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["cpe_country", "upgrade_status"],
                                     aggregation_name=self._component_name + ".cpe_count_with_upgrade_status"))

    def distinct_cpe_count_recently_used_settings_items(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    "viewer_id",
                    explode("`profile.recentlyUsedSettingsItems`").alias("settings_items")) \
            .aggregate(DistinctCount(aggregation_field="viewer_id",
                                     group_fields=["settings_items"],
                                     aggregation_name=self._component_name + ".recently_used"))

    def distinct_cpe_with_age_restriction_enabled(self, common_settings_pipeline):
        return common_settings_pipeline \
            .where("`profile.ageLock` is not NULL") \
            .withColumn("profile_age_lock", col("`profile.ageLock`"), ) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["profile_age_lock"],
                                     aggregation_name=self._component_name + ".cpe_with_age_restriction"))

    def distinct_cpe_with_selected_audio_track_language(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    col("`profile.audioLang`").alias("profile_audio_lang"),
                    "viewer_id") \
            .where("profile_audio_lang is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["profile_audio_lang"],
                                     aggregation_name=self._component_name + ".cpe_with_selected_audio_track_language"))

    def distinct_cpe_with_selected_subtitles_track_language(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    col("`profile.subLang`").alias("profile_sub_lang"),
                    "viewer_id") \
            .where("profile_sub_lang is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["profile_sub_lang"],
                                     aggregation_name=self._component_name + ".cpe_with_selected_subtitles_language"))

    def distinct_cpe_factory_reset_report(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    col("`cpe.factoryResetState`").alias("cpe_factory_reset_state"),
                    "viewer_id") \
            .where("cpe_factory_reset_state is not NULL") \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["cpe_factory_reset_state"],
                                     aggregation_name=self._component_name))

    def distinct_tv_brands_paired_with_each_cpe(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("tv").getItem("brand").alias("brand"),
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("tv").getItem("isPaired").alias("is_paired"),
                    "viewer_id") \
            .where("brand is not NULL") \
            .withColumn("brand", when(col("brand") == "", "unknown_brands").otherwise(col("brand"))) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["is_paired", "brand"],
                                     aggregation_name=self._component_name + ".tv_brands_paired_with_each_cpe"))

    def distinct_audio_brands_paired_with_each_cpe(self, common_settings_pipeline):
        return common_settings_pipeline \
            .select("@timestamp",
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("amp").getItem("brand").alias("brand"),
                    col("`cpe.quicksetPairedDevicesInfo`").getItem("amp").getItem("isPaired").alias("is_paired"),
                    "viewer_id") \
            .where("brand is not NULL") \
            .withColumn("brand", when(col("brand") == "", "unknown_brands").otherwise(col("brand"))) \
            .aggregate(DistinctCount(aggregation_field="viewer_id", group_fields=["is_paired", "brand"],
                                     aggregation_name=self._component_name + ".audio_brands_paired_with_each_cpe"))


def create_processor(configuration):
    """
    Method to create the instance of the Settings report processor
    """
    return CpeSettingsReportEventProcessor(configuration, CpeSettingsReportEventProcessor.create_schema())


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
