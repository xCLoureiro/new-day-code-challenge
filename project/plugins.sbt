resolvers += Classpaths.sbtPluginReleases

// SBT assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.2")

// helpful scala tools
addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.0.4")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

// IDE integrations
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")