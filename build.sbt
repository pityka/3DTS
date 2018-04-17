scalaVersion := "2.12.4"

resolvers ++= Seq[Resolver](
  "jzy3d maven repository " at "http://maven.jzy3d.org/releases")

libraryDependencies ++= Seq(
  "io.github.pityka" %% "fileutils" % "1.1.1",
  "io.github.pityka" %% "stringsplit" % "1.0.0",
  "io.github.pityka" %% "saddle-linalg" % "0.0.23",
  "io.github.pityka" %% "poibin" % "0.0.1",
  "io.github.pityka" %% "intervaltree" % "1.0.0",
  "tasks-core" %% "tasks-core" % "0.0.10",
  "tasks-upickle" %% "tasks-upickle" % "0.0.10",
  "tasks-collection" %% "tasks-collection" % "0.0.11",
  "io.github.pityka" % "saddle-core-fork_2.12" % "1.3.4-fork1",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.spire-math" %% "spire" % "0.13.0",
  "com.github.samtools" % "htsjdk" % "2.14.3",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "com.thesamet" %% "kdtree" % "1.0.6-hli1",
  "org.jzy3d" % "jzy3d-api" % "1.0.0" % "test",
  "indexlib" %% "indexlib" % "1.2.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5"
).map(
  x =>
    // x.exclude("log4j", "log4j")
    x.exclude("commons-logging", "commons-logging")
      .excludeAll(ExclusionRule(organization = "ch.qos.logback"))
)

enablePlugins(JavaAppPackaging)

resources in Compile += (fastOptJS in Compile in browser).value.data

lazy val browser = (project in file("browser"))
  .settings(
    scalaVersion := "2.12.4",
    libraryDependencies ++= Seq(
      "org.scala-js" %%% "scalajs-dom" % "0.9.5",
      "com.lihaoyi" %%% "scalatags" % "0.6.7",
      "com.lihaoyi" %%% "scalarx" % "0.3.2",
      "be.doeraene" %%% "scalajs-jquery" % "0.9.3",
      "com.lihaoyi" %%% "upickle" % "0.4.4"
    )
  )
  .dependsOn(sharedJs)
  .enablePlugins(ScalaJSPlugin)

val shared =
  (crossProject.crossType(CrossType.Pure) in file("shared"))
    .settings(
      scalaVersion := "2.12.4"
    )
    .settings(
      libraryDependencies ++= Seq("com.lihaoyi" %%% "upickle" % "0.4.4"))
    .jsSettings(
      libraryDependencies ++= Seq(
        "org.scala-js" %%% "scalajs-java-time" % "0.2.0")
    )

lazy val root =
  project.in(file(".")).dependsOn(sharedJVM).settings(name := "saturation")

lazy val sharedJVM = shared.jvm
lazy val sharedJs = shared.js

mainClass in Compile := Some("ProteinDepletion")

scalafmtOnCompile in ThisBuild := true
