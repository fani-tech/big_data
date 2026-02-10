FROM apache/spark:3.5.8

COPY core-site.xml /opt/spark/conf/core-site.xml
COPY hdfs-site.xml /opt/spark/conf/hdfs-site.xml

ENV HDFS_USERNAME=
ENV USERNAME=

ENTRYPOINT ["/bin/bash", "-c", "\
  U=\"${HDFS_USERNAME:-$USERNAME}\"; \
  if [ -z \"$U\" ]; then \
    echo 'ERROR: Πρέπει να ορίσετε HDFS_USERNAME (ή USERNAME ως fallback)' >&2; \
    exit 1; \
  fi; \
  export LOG_DIR=hdfs://hdfs-namenode:9000/user/$U/logs; \
  export SPARK_HISTORY_OPTS=\"-Dspark.history.fs.logDirectory=$LOG_DIR \
                             -Dspark.history.fs.cleaner.enabled=true \
                             -Dspark.history.ui.port=18080\"; \
  echo \"Ξεκινάει το Spark History Server για τον χρήστη $U, logs από $LOG_DIR\"; \
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"]
