name := "new-day-code-challenge"
organization := "com.github.xcloureiro"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.6"


libraryDependencies ++= Seq(
  // Spark libs
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-sql" % "1.6.2",
  "org.apache.spark" %% "spark-hive" % "1.6.2" % "provided",

  // testing
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)


parallelExecution in Test := false


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}