#!/bin/sh

MOVIES_FILE=/root/data/in/movies.dat
RATINGS_FILE=/root/data/in/ratings.dat
OUT_DIR=/root/data/out

JAR=/root/target/scala-2.10/new-day-code-challenge-assembly-1.0.0-SNAPSHOT.jar

spark-submit \
--master local[*] \
--name new-day-code-challenge \
--driver-java-options "-XX:+HeapDumpOnOutOfMemoryError -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector" \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector" \
--driver-cores 1 \
--driver-memory 1g \
--num-executors 4 \
--executor-memory 1g \
--executor-cores 2 \
--class com.github.xcloureiro.newday.Solution \
$JAR $MOVIES_FILE $RATINGS_FILE $OUT_DIR > ./new-day-code-challenge.log 2>&1 &
echo $! >./new-day-code-challenge.pid &