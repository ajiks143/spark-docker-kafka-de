FROM mesosphere/spark:2.12.0-3.0.1-scala-2.12-hadoop-3.2

COPY . /app/xapo/
ENV PYTHONPATH /app/xapo/
RUN chmod 777 /app/xapo/
WORKDIR /app/xapo/
ENTRYPOINT /app/xapo/entrypoint.sh

