from abc import ABCMeta, abstractmethod
from pyspark.sql.functions import from_json, col, collect_list, struct, udf, explode, lit
from pyspark.sql.types import ArrayType, StringType

class StbProcessor(object):
    """

    """
    __metaclass__ = ABCMeta

    def __init__(self, configuration, schema):

        self.__configuration = configuration
        self._schema = schema
        self._component_name = configuration.property("analytics.componentName")

    @abstractmethod
    def _process_pipeline(self, streams):
        """
        Abstract  pipeline method
        :param json_stream: kafka stream reader
        :return: pipeline dataframe
        """

    def create(self, read_stream):

        json_stream = read_stream \
            .select(from_json(read_stream["value"].cast("string"), self._schema).alias("json")) \
            .select("json.*") \
            .filter(col("MemoryUsage_totalKb") != "{}")

        return [self._convert_to_kafka_structure(dataframe) for dataframe in self._process_pipeline(json_stream)]

    def _convert_to_kafka_structure(self, dataframe):
        return dataframe \
            .selectExpr("to_json(struct(*)) AS value") \
            .withColumn("topic", lit(self.kafka_output))

    @staticmethod
    def udf_filter(top_procs):
        """
        Remove any rows which do not contain top processes.
        :param x:
        :return:
        """
        count = len([x for x in top_procs if x == ""])
        if count == len(top_procs):
            return False
        else:
            return True

    def _fill_df(self, agg_stream):
        """
        Split into Memory stream and Top Processes stream
        :param agg_stream:
        :return:
        """

        def udf_fill_memory_field(row):
            timestamp = row[1]
            MemoryUsage_freeKb = row[2]
            MemoryUsage_totalKb = row[3]
            TopProcesses_processes = row[4]
            res = []
            for proc in TopProcesses_processes:
                if proc != "":
                    proc_row = []
                    ix = TopProcesses_processes.index(proc)
                    proc_row.append(timestamp[ix])
                    if MemoryUsage_totalKb[ix] == '0':
                        mem_ix = ''
                        for i in list(reversed(range(ix))):
                            if MemoryUsage_totalKb[i] != '0':
                                mem_ix = i
                                break

                        if mem_ix != '':
                            proc_row.append(MemoryUsage_freeKb[mem_ix])
                            proc_row.append(MemoryUsage_totalKb[mem_ix])
                            proc_row.append(proc)
                            res.append(proc_row)
                    else:
                        proc_row.append(MemoryUsage_freeKb[ix])
                        proc_row.append(MemoryUsage_totalKb[ix])
                        proc_row.append(proc)
                        res.append(proc_row)

            return res

        fill_mem_field = udf(lambda row: udf_fill_memory_field(row), ArrayType(ArrayType(StringType())))
        filled_df = agg_stream \
            .withColumn("res", fill_mem_field(struct([agg_stream[x] for x in agg_stream.columns]))) \
            .drop('collect_list(timestamp)', 'collect_list(MemoryUsage_freeKb)',
                  'collect_list(MemoryUsage_totalKb)', 'collect_list(TopProcesses_processes)') \
            .withColumn('res', explode('res')) \
            .withColumn('timestamp', col('res').getItem(0)) \
            .withColumn('MemoryUsage_freeKb', col('res').getItem(1)) \
            .withColumn('MemoryUsage_totalKb', col('res').getItem(2)) \
            .withColumn('TopProcesses_processes', col('res').getItem(3)) \
            .drop('res')

        return filled_df