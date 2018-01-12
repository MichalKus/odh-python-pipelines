FROM smartislav/spark:2.2.1-1

RUN pip install nose nose-exclude coverage \
 && mkdir -p /odh/python/.xunit-reports /odh/python/.coverage-reports

RUN export http_proxy=$https_proxy && export https_proxy=$https_proxy \
    && apt-get -y update && apt-get -y install wget \
    && wget http://ftp.ru.debian.org/debian/pool/main/o/openssl/libssl1.0.0_1.0.1t-1+deb8u7_amd64.deb \
    && dpkg -i libssl1.0.0_1.0.1t-1+deb8u7_amd64.deb && apt-get remove -y wget \
    && rm -f libssl1.0.0_1.0.1t-1+deb8u7_amd64.deb

ENV SPARK_HOME=/spark
ENV PYTHONPATH=/odh/python:$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
ENV TIMEZONES_ROOT_DIR=/odh/python/resources/timezones

ADD src /odh/python
RUN chmod -R 644 /odh/python
