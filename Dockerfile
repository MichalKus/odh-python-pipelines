FROM smartislav/spark:2.2.0-12

RUN pip install nose nose-exclude coverage \
 && mkdir -p /odh/python/.xunit-reports

ENV SPARK_HOME=/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH

ADD src /odh/python
