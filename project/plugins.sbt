scalacOptions += "-deprecation"

resolvers += Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

resolvers += Resolver.sonatypeRepo("releases")

resolvers += Resolver.typesafeRepo("releases")

resolvers += "Era7 maven releases" at "http://releases.era7.com.s3.amazonaws.com"

addSbtPlugin("com.ambiata" % "promulgate" % "0.11.0-20141014013725-80c129f")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.0.1")
