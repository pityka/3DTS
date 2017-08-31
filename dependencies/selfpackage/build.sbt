lazy val commonSettings = Seq(
    organization := "io.github.pityka",
    scalaVersion := "2.11.8",
    version := "0.0.1"
  ) ++ reformatOnCompileSettings

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "selfpackage",
    libraryDependencies ++= Seq(
      "io.github.lukehutch" % "fast-classpath-scanner" % "2.0.19",
      "org.scalatest" %% "scalatest" % "2.1.5" % "test"),
    licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
    pomExtra in Global := {
      <url>https://pityka.github.io/selfpackage/</url>
     <scm>
       <connection>scm:git:github.com/pityka/selfpackage</connection>
       <developerConnection>scm:git:git@github.com:pityka/selfpackage</developerConnection>
       <url>github.com/pityka/selfpackage</url>
     </scm>
     <developers>
       <developer>
         <id>pityka</id>
         <name>Istvan Bartha</name>
         <url>https://pityka.github.io/selfpackage/</url>
       </developer>
     </developers>
    }
  )

lazy val testProject = (project in file("test"))
  .settings(commonSettings: _*)
  .settings(
    name := "selfpackage-test",
    publishArtifact := false
  )
  .dependsOn(root)
  .enablePlugins(JavaAppPackaging)
