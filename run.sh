#!/bin/bash
if sbt package
then
~/bin/spark-1.6.1-bin-hadoop2.6/bin/spark-submit \
  --packages datastax:spark-cassandra-connector:1.6.0-M1-s_2.10 \
  --conf spark.cassandra.connection.host=localhost \
  --conf "spark.default.parallelism=12" \
  --driver-memory 1g --class "Count" target/scala-2.10/keyword-count_2.10-0.0.1.jar
fi
