FROM smartislav/spark:spark-2.2.1-jdk8u121

RUN pip install nose nose-exclude coverage \
 && mkdir -p /odh/python/.xunit-reports /odh/python/.coverage-reports
RUN pip install pytz==2018.4

ENV SPARK_HOME=/spark
ENV PYTHONPATH=/odh/python:$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
ENV TIMEZONES_ROOT_DIR=/odh/python/resources/timezones

ADD src /odh/python
RUN chmod -R 644 /odh/python
