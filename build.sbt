name := "spark-data-profiler"

version := "1.0.0"

scalaVersion := "2.11.8"

sparkVersion := "2.3.0"

sparkComponents += "sql"

sparkComponents ++= Seq("sql","hive")

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

parallelExecution in Test := false