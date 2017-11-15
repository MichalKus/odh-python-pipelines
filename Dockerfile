FROM smartislav/spark:2.2.0-12

RUN pip install nose \
 && pip install nose-exclude \
 && pip install coverage

ENV SPARK_HOME=/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

ADD src /odh/python
