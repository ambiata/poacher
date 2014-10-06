import sbt._
import Keys._
import sbt.KeyRanks._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.ambiata.promulgate.project.ProjectPlugin._

object build extends Build {
  type Settings = Def.Setting[_]

  lazy val poacher = Project(
    id = "poacher"
  , base = file(".")
  , settings = 
    standardSettings ++ 
    promulgate.library(s"com.ambiata.poacher", "ambiata-oss") ++
    Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.scoobi(version.value) ++ depend.hadoop(version.value) ++ depend.specs2)
  )

  lazy val standardSettings = Defaults.defaultSettings ++
                              projectSettings          ++
                              compilationSettings      ++
                              testingSettings          ++
                              Seq[Settings](
                                resolvers := depend.resolvers
                              )

  lazy val projectSettings: Seq[Settings] = Seq(
    name := "poacher"
  , version in ThisBuild := s"""1.0.0-${Option(System.getenv("HADOOP_VERSION")).getOrElse("cdh5")}"""
  , organization := "com.ambiata"
  , scalaVersion := "2.11.2"
  , crossScalaVersions := Seq("2.10.4", "2.11.2")
  , fork in run  := true
  )

  lazy val compilationSettings: Seq[Settings] = Seq(
    javaOptions ++= Seq("-Xmx3G", "-Xms512m", "-Xss4m")
    , javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
    , maxErrors := 20
    , scalacOptions ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature", "-language:_", "-Xlint", "-Xfatal-warnings", "-Yinline-warnings")
  )

  lazy val testingSettings: Seq[Settings] = Seq(
    initialCommands in console := "import org.specs2._"
  , logBuffered := false
  , cancelable := true
  , fork in test := true
  , javaOptions += "-Xmx3G"
  , testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "tracefilter", "/.*specs2.*,.*mundane.testing.*")
  )

  lazy val buildAssemblySettings: Seq[Settings] = Seq(
    artifact in (Compile, assembly) ~= { art =>
      art.copy(`classifier` = Some("assembly"))
    }
  ) ++ addArtifact(artifact in (Compile, assembly), assembly)
}
