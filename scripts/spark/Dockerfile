FROM apache/spark:3.5.0

USER root

COPY *.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

USER 185
