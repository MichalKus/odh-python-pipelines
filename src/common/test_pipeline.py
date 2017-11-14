from common.abstract_pipeline import AbstractPipeline


class TestPipeline(AbstractPipeline):
    """
    Base class for structured streaming pipeline, which read data from file and write to memory
    """

    def __init__(self, configuration, processor, input_dir, table_name):
        self.__input_dir = input_dir
        self.__table_name = table_name
        super(TestPipeline, self).__init__(configuration, processor)

    def _create_custom_read_stream(self, spark):
        return spark.readStream.text(self.__input_dir)

    def _create_custom_write_streams(self, pipelines):
        result = []
        for index, pipeline in enumerate(pipelines):
            result.append(pipeline.writeStream.format("memory")
                          .outputMode(self._output_mode)
                          .queryName(self.__table_name + str(index)))
        return result
