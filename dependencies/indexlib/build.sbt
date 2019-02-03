val commonSettings = Seq(
  scalaVersion := "2.12.8",
  version := "1.3.0"
)

lazy val root = project
  .in(file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "indexlib",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.google.guava" % "guava" % "19.0",
      "org.xerial.snappy" % "snappy-java" % "1.1.2.1",
      "com.spotify.sparkey" % "sparkey" % "2.1.3"
    )
  )
