#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_HOME="${SPARK_HOME:-/usr/local/spark-2.4.6}"
SCALA_HOME="${SCALA_HOME:-/usr/local/scala-2.11.12}"
TARGET_DIR="$ROOT_DIR/SparkMain/target"
MAIN_JAR="${USER_BEHAVIOR_JAR:-$TARGET_DIR/userbehavior-realtime.jar}"
TEST_CLASSES="$TARGET_DIR/userbehavior-realtime-test-classes"
TEST_JAR="$TARGET_DIR/userbehavior-realtime-test.jar"

bash "$ROOT_DIR/build_user_behavior_realtime.sh"

rm -rf "$TEST_CLASSES"
mkdir -p "$TEST_CLASSES"
SPARK_CP="$(printf "%s:" "$SPARK_HOME"/jars/*.jar)"

"$SCALA_HOME/bin/scalac" \
    -target:jvm-1.8 \
    -classpath "$SPARK_CP$MAIN_JAR" \
    -d "$TEST_CLASSES" \
    "$ROOT_DIR/SparkMain/src/test/scala/org/bigdata/streaming/UserBehaviorRealtimeSmokeTest.scala"

(
    cd "$TEST_CLASSES"
    jar cf "$TEST_JAR" org/bigdata/streaming/*.class
)

"$SPARK_HOME/bin/spark-submit" \
    --conf spark.ui.showConsoleProgress=false \
    --class org.bigdata.streaming.UserBehaviorRealtimeSmokeTest \
    --master local[2] \
    --jars "$MAIN_JAR" \
    "$TEST_JAR"
