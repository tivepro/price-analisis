name := "test"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0",
  ("org.apache.spark" %% "spark-streaming-kafka" % "1.6.0").
    exclude("org.spark-project.spark", "unused")
)