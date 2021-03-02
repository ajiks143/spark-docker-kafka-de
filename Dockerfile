FROM mesosphere/spark:2.12.0-3.0.1-scala-2.12-hadoop-3.2

WORKDIR /usr/xapo/

COPY . /usr/xapo/

ENV PYTHONPATH /usr/xapo/

