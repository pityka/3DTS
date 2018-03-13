scalaVersion := "2.11.11"

resolvers ++= Seq[Resolver](
  "jzy3d maven repository " at "http://maven.jzy3d.org/releases")

libraryDependencies ++= Seq(
  "io.github.pityka" %% "fileutils" % "1.1.0",
  "io.github.pityka" %% "stringsplit" % "1.0.0",
  "io.github.pityka" %% "nspl-core" % "0.0.14-hli3",
  "io.github.pityka" %% "saddle-linalg" % "0.0.15",
  "io.github.pityka" %% "poibin" % "0.0.1",
  "io.github.pityka" %% "intervaltree" % "1.0.0",
  "tasks-core" %% "tasks-core" % "0.0.8-hli4",
  "org.scala-saddle" %% "saddle-core" % "1.3.4",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.spire-math" %% "spire" % "0.13.0",
  "com.github.samtools" % "htsjdk" % "1.140",
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
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

reformatOnCompileSettings

enablePlugins(JavaAppPackaging)

resources in Compile += (fastOptJS in Compile in browser).value.data

lazy val browser = (project in file("browser"))
  .settings(
    scalaVersion := "2.11.11",
    libraryDependencies ++= Seq(
      "org.scala-js" %%% "scalajs-dom" % "0.9.0",
      "com.lihaoyi" %%% "scalatags" % "0.5.5",
      "com.lihaoyi" %%% "scalarx" % "0.3.1",
      "be.doeraene" %%% "scalajs-jquery" % "0.9.0",
      "com.lihaoyi" %%% "upickle" % "0.4.4",
      "io.github.pityka" %%% "nspl-scalatags-js" % "0.0.12"
    )
  )
  .settings(reformatOnCompileSettings: _*)
  .dependsOn(sharedJs)
  .enablePlugins(ScalaJSPlugin)

val shared =
  (crossProject.crossType(CrossType.Pure) in file("shared"))
    .settings(
      scalaVersion := "2.11.11"
    )
    .settings(reformatOnCompileSettings: _*)
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
