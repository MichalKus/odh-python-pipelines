"""
Basic analytics driver for STB Graphic Memory usage report.
"""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from common.basic_analytics.aggregations import Avg
from common.basic_analytics.basic_analytics_processor import BasicAnalyticsProcessor
from util.kafka_pipeline_helper import start_basic_analytics_pipeline
from pyspark.sql.functions import col, lower, lit, when


class GraphicMemoryStbBasicAnalytics(BasicAnalyticsProcessor):
    """
    Basic analytics driver for STB Graphic Memory usage report.
    """

    def _process_pipeline(self, json_stream):
        stream = json_stream \
            .withColumnRenamed("GraphicsMemoryUsage.totalKb", "totalKb") \
            .withColumnRenamed("GraphicsMemoryUsage.peakKb", "peakKb") \
            .withColumnRenamed("GraphicsMemoryUsage.freeKb", "freeKb") \
            .withColumnRenamed("GraphicsMemoryUsage.mapping", "mapping") \
            .withColumn("mapping", when(col("mapping") == "CRR (SECURE)", "crr_secure")
                        .when(col("mapping") == "GFX", "gfx")
                        .when(col("mapping") == "MAIN", "main")
                        .when(col("mapping") == "PICBUF0", "picbuf0")
                        .when(col("mapping") == "PICBUF1", "picbuf0")
                        .when(col("mapping") == "SAGE (SECURE)", "sage_secure")
                        .otherwise("unclassified")) \
            .where("mapping != 'unclassified'")

        return [stream.aggregate(Avg(group_fields="mapping", aggregation_field=field,
                                     aggregation_name=self._component_name))
                for field in ["totalKb", "peakKb", "freeKb"]]

    @staticmethod
    def create_schema():
        return StructType([
            StructField("@timestamp", TimestampType()),
            StructField("GraphicsMemoryUsage.peakKb", StringType()),
            StructField("GraphicsMemoryUsage.totalKb", StringType()),
            StructField("GraphicsMemoryUsage.freeKb", StringType()),
            StructField("GraphicsMemoryUsage.mapping", StringType())
        ])


def create_processor(configuration):
    """Method to create the instance of the processor"""
    return GraphicMemoryStbBasicAnalytics(configuration, GraphicMemoryStbBasicAnalytics.create_schema())


if __name__ == '__main__':
    start_basic_analytics_pipeline(create_processor)
