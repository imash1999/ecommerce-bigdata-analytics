FROM flink:1.18.0-scala_2.12-java11

COPY *.jar /opt/flink/lib/
COPY submit.sql /opt/flink/submit.sql

CMD ["/opt/flink/bin/sql-client.sh", "-f", "/opt/flink/submit.sql"]
