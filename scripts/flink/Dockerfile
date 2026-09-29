FROM flink:1.17-java11

COPY submit.sql /opt/flink/submit.sql

CMD ["bash", "-c", "while ! timeout 1 bash -c '</dev/tcp/flink-jobmanager/8081' 2>/dev/null; do echo 'Waiting for JobManager...'; sleep 2; done; /opt/flink/bin/sql-client.sh -endpoint flink-jobmanager:8081 -l /opt/flink/usrlib -f /opt/flink/submit.sql; tail -f /dev/null"]
