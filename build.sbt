ThisBuild / scalaVersion := "2.12.20"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "dev.noorps"

lazy val root = (project in file("."))
  .settings(
    name := "fleetlake",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.6" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.6" % "provided",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true
  )
