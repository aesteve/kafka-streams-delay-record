ThisBuild / version := "0.0.1-SNAPSHOT"
ThisBuild / scalaVersion := "3.2.1"
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

lazy val root = (project in file("."))
  .settings(
    name := "rename-topic-streams",
    idePackagePrefix := Some("com.github.aesteve")
  )

val kafkaVersion = "3.3.1"
val scalatestVersion = "3.2.14"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion


libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVersion % Test
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % Test