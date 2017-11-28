organization := "at.hazm"

name := "samplecode.fusiontables"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.google.apis" % "google-api-services-fusiontables" % "v2-rev19-1.23.0",
  "com.google.apis" % "google-api-services-drive" % "v3-rev90-1.21.0"
)
