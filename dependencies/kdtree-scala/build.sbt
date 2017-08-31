organization := "com.thesamet"

name := "kdtree"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

scalacOptions ++= Seq("-unchecked", "-deprecation")

scalaVersion := "2.11.11"

crossScalaVersions := Seq("2.12.2", "2.11.11")
