import sbt._
import Keys._

object depend {
  val scalaz    = Seq("org.scalaz"           %% "scalaz-core"     % "7.0.6",
                      "org.scalaz"           %% "scalaz-effect"   % "7.0.6")
  val scopt     = Seq("com.github.scopt"     %% "scopt"           % "3.2.0")
  val trove     = Seq("trove"                %  "trove"           % "1.0.2")
  val joda      = Seq("joda-time"            %  "joda-time"       % "2.1",
                      "org.joda"             %  "joda-convert"    % "1.1")
  val specs2    = Seq("org.specs2"           %% "specs2-core",
                      "org.specs2"           %% "specs2-junit",
                      "org.specs2"           %% "specs2-html",
                      "org.specs2"           %% "specs2-matcher-extra",
                      "org.specs2"           %% "specs2-scalacheck").map(_ % "2.3.10")
  val commonsio = Seq("commons-io"           %  "commons-io"      % "2.4")
  val thrift    = Seq("org.apache.thrift"    %  "libthrift"       % "0.9.1")
  val saws      = Seq("com.ambiata"          %% "saws"            % "1.2.1-20140604032927-def9ad2",
                      "net.java.dev.jets3t"  %  "jets3t"          % "0.9.0" )
  val mundane   = Seq("com.ambiata"          %% "mundane"         % "1.2.1-20140505014353-a175c40")

  def scoobi(version: String) = {
    val scoobiVersion =
      if (version.contains("cdh3"))      "0.9.0-cdh3-20140521064448-df53463"
      else if (version.contains("cdh4")) "0.9.0-cdh4-20140521064733-df53463"
      else if (version.contains("cdh5")) "0.9.0-cdh5-20140521065009-df53463"
      else                               "0.9.0-cdh5-20140521065009-df53463"


    Seq("com.nicta" %% "scoobi" % scoobiVersion)
  }

  val resolvers = Seq(
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("public"),
      Resolver.typesafeRepo("releases"),
      "cloudera"             at "https://repository.cloudera.com/content/repositories/releases",
      Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns),
      "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases")
}
