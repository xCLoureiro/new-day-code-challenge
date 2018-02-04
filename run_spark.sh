#!/bin/sh

if [ "$#" -ne 4 ]; then
    echo "Application usage: ./run_spark.sh <jar_file> <moviesFile> <ratingsFile> <outDir (without last /)>"
    exit 1
fi

JAR=$1

MOVIES_FILE=$2
RATINGS_FILE=$3
OUT_DIR=$4

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
