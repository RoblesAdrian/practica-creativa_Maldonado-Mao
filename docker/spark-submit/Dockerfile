FROM apache/spark:4.1.2-scala2.13-java17-python3-r-ubuntu

USER root

RUN python3 -m pip install --no-cache-dir numpy

COPY jars/ /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER 185
