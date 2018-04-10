import sbt.Keys._
import sbt._

lazy val root = (project in file(".")).settings(
  name := "Dota Recommendation with Spark",
  version := "1.0",
  scalaVersion := "2.11.8",
  mainClass in Compile := Some("sparkdota.SparkDota"),
  dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5",
  dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5",
 dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5",

libraryDependencies += "junit" % "junit" % "4.10" % Test,
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "com.typesafe.play" %% "play-json" % "2.6.9"
),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)