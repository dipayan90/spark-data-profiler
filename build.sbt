name := "spark-data-profiler"
version := "1.0.0"
description := "Profiles Data From Source and generates report"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" ,
  "org.apache.spark" %% "spark-sql" % "2.3.0" ,
  "org.scalactic" %% "scalactic" % "3.0.5" ,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

parallelExecution in Test := false