ThisBuild / version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15"
val sparkVersion = "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "scala training"
  )
