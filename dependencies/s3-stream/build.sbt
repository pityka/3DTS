val akkaVersion = "2.4.17"
val akkaHttpVersion = "10.0.5"
val scalatestVersion = "2.2.6"
val akka = "com.typesafe.akka" %% "akka-actor" % akkaVersion
val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion
val akkaHttpXML = "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion
val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion

lazy val commonSettings = Seq(
    organization := "io.github.pityka",
    version := "0.0.2-hli5",
    scalaVersion := "2.11.8",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    licenses += ("Apache-2.0", url(
      "https://opensource.org/licenses/Apache-2.0")),
    pomExtra in Global := {
    <url>https://pityka.github.io/s3-stream/</url>
      <scm>
        <connection>scm:git:github.com/pityka/s3-stream</connection>
        <developerConnection>scm:git:git@github.com:pityka/s3-stream</developerConnection>
        <url>github.com/pityka/s3-stream</url>
      </scm>
      <developers>
        <developer>
          <id>pityka</id>
          <name>Istvan Bartha</name>
          <url>https://pityka.github.io/s3-stream/</url>
        </developer>
      </developers>
  }
  ) ++ reformatOnCompileSettings

lazy val root = (project in file("."))
  .settings(publishArtifact := false)
  .aggregate(s3stream, awsRequests)

lazy val awsRequests = (project in file("akka-http-aws"))
  .settings(commonSettings: _*)
  .settings(
    name := "akka-http-aws-fork",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "akka-http-unboundedqueue" % "1.0.0",
      akkaHttp,
      akkaHttpSprayJson,
      scalatest % Test,
      akkaStreamTestkit % Test)
  )

lazy val s3stream = (project in file("s3-stream"))
  .settings(commonSettings: _*)
  .settings(
    name := "s3-stream-fork",
    libraryDependencies ++= Seq(
      akkaStream,
      akkaHttp,
      akkaHttpXML,
      "io.github.pityka" %% "akka-http-unboundedqueue" % "1.0.0",
      "org.scalaj" %% "scalaj-http" % "2.3.0",
      akkaTestkit % Test,
      akkaStreamTestkit % Test,
      scalatest % Test)
  )
  .dependsOn(awsRequests)
