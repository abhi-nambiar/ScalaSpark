ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaSpark"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"