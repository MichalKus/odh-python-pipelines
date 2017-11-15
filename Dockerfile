FROM smartislav/spark:2.2.0-12

ADD src /odh/python

RUN pip install nose
