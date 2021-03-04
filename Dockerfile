FROM mesosphere/spark:2.12.0-3.0.1-scala-2.12-hadoop-3.2

COPY . /app/xapo/
ENV PYTHONPATH /app/xapo/

RUN apt-get update && apt-get install -y \
    software-properties-common
RUN add-apt-repository universe
RUN apt-get update && apt-get install -y \
    python3.6 \
    python3-pip

RUN chmod 777 /app/xapo/
RUN pip3 install -r /app/xapo/requirements.txt
WORKDIR /app/xapo/
ENTRYPOINT /app/xapo/entrypoint.sh
