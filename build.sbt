scalaVersion := "2.12.5"

val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:postfixOps",
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Xlint:unsound-match", // Pattern match may not be typesafe.
    "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
    "-Ypartial-unification", // Enable partial unification in type constructor inference
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
    "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
    // "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates" // Warn if a private member is unused.
  )
)

libraryDependencies ++= Seq(
  "io.github.pityka" %% "fileutils" % "1.1.1",
  "io.github.pityka" %% "stringsplit" % "1.1.0",
  "io.github.pityka" %% "saddle-linalg" % "0.0.23",
  "io.github.pityka" %% "poibin" % "0.0.1",
  "io.github.pityka" %% "intervaltree" % "1.0.0",
  "io.github.pityka" %% "sampling" % "0.0.1",
  "io.github.pityka" %% "saddle-core-fork" % "1.3.4-fork1",
  "io.github.pityka" %% "tasks-core" % "0.0.16-SNAPSHOT",
  "io.github.pityka" %% "tasks-upickle" % "0.0.16-SNAPSHOT",
  "io.github.pityka" %% "tasks-collection" % "0.0.16-SNAPSHOT",
  "indexlib" %% "indexlib" % "1.2.0",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.apache.commons" % "commons-compress" % "1.16.1",
  "org.spire-math" %% "spire" % "0.13.0",
  "com.github.samtools" % "htsjdk" % "2.14.3",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "com.thesamet" %% "kdtree" % "1.0.6-hli1",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.11"
).map(
  x =>
    x.exclude("log4j", "log4j")
      .exclude("commons-logging", "commons-logging")
)

resolvers += Resolver.sonatypeRepo("public")

enablePlugins(JavaAppPackaging)

resources in Compile += (fastOptJS in Compile in browser).value.data

lazy val browser = (project in file("browser"))
  .settings(commonSettings: _*)
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
    .settings(commonSettings: _*)
    .jsSettings(
      libraryDependencies ++= Seq(
        "org.scala-js" %%% "scalajs-java-time" % "0.2.0")
    )

lazy val root =
  project
    .in(file("."))
    .settings(commonSettings: _*)
    .dependsOn(sharedJVM)
    .settings(name := "saturation")

lazy val sharedJVM = shared.jvm
lazy val sharedJs = shared.js

mainClass in Compile := Some("sd.ProteinDepletion")

scalafmtOnCompile in ThisBuild := true
