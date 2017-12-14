FROM smartislav/spark:2.2.0-12

RUN pip install nose nose-exclude coverage \
 && mkdir -p /odh/python/.xunit-reports /odh/python/.coverage-reports

ENV SPARK_HOME=/spark
ENV PYTHONPATH=/odh/python:$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
ENV TIMEZONES_ROOT_DIR=/odh/python/resources/timezones

ADD src /odh/python
RUN chmod -R 644 /odh/python
