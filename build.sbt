val catsV                = "2.1.1"
val jmsV                 = "2.0.1"
val ibmMQV               = "9.2.0.0"
val activeMQV            = "2.15.0"
val catsEffectV          = "2.1.4"
val catsEffectScalaTestV = "0.4.1"
val fs2V                 = "2.4.4"
val log4catsV            = "1.1.1"
val log4jSlf4jImplV      = "2.13.3"

val kindProjectorV    = "0.11.0"
val betterMonadicForV = "0.3.1"

// Projects
lazy val jms4s = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, ibmMQ, activeMQArtemis, tests, examples, site)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(name := "jms4s")
  .settings(parallelExecution in Test := false)

lazy val ibmMQ = project
  .in(file("ibm-mq"))
  .settings(commonSettings)
  .settings(name := "jms4s-ibm-mq")
  .settings(libraryDependencies += "com.ibm.mq" % "com.ibm.mq.allclient" % ibmMQV)
  .settings(parallelExecution in Test := false)
  .dependsOn(core)

lazy val activeMQArtemis = project
  .in(file("active-mq-artemis"))
  .settings(commonSettings)
  .settings(name := "jms4s-active-mq-artemis")
  .settings(libraryDependencies += "org.apache.activemq" % "artemis-jms-client-all" % activeMQV)
  .settings(parallelExecution in Test := false)
  .dependsOn(core)

lazy val tests = project
  .in(file("tests"))
  .settings(commonSettings: _*)
  .enablePlugins(NoPublishPlugin)
  .settings(libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jSlf4jImplV % Runtime)
  .settings(parallelExecution in Test := false)
  .dependsOn(ibmMQ, activeMQArtemis)

lazy val examples = project
  .in(file("examples"))
  .settings(commonSettings: _*)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(ibmMQ, activeMQArtemis)

lazy val site = project
  .in(file("site"))
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .dependsOn(core, ibmMQ, activeMQArtemis)
  .settings {
    import microsites._
    Seq(
      micrositeName := "jms4s",
      micrositeDescription := "a functional wrapper for jms",
      micrositeAuthor := "fp-in-bo",
      micrositeGithubOwner := "fp-in-bo",
      micrositeGithubRepo := "jms4s",
      micrositeBaseUrl := "/jms4s",
      micrositeFooterText := None,
      micrositeGitterChannel := false,
      micrositeCompilingDocsTool := WithMdoc,
      scalacOptions in Tut --= Seq(
        "-Xfatal-warnings",
        "-Ywarn-unused-import",
        "-Ywarn-unused:imports",
        "-Ywarn-numeric-widen",
        "-Ywarn-dead-code",
        "-Xlint:-missing-interpolator,_"
      ),
      micrositePushSiteWith := GitHub4s,
      micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
      micrositeExtraMdFiles := Map(
        file("README.md") -> ExtraMdFileConfig(
          "index.md",
          "home",
          Map("section" -> "home", "position" -> "0", "permalink" -> "/")
        ),
        file("CODE_OF_CONDUCT.md") -> ExtraMdFileConfig(
          "code-of-conduct.md",
          "page",
          Map("title" -> "Code of conduct", "section" -> "code of conduct", "position" -> "100")
        ),
        file("LICENSE") -> ExtraMdFileConfig(
          "license.md",
          "page",
          Map("title" -> "License", "section" -> "license", "position" -> "101")
        )
      )
    )
  }

// General Settings
lazy val commonSettings = Seq(
  scalaVersion := "2.13.1",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.10"),
  scalafmtOnCompile := true,
  addCompilerPlugin("org.typelevel" %% "kind-projector"     % kindProjectorV cross CrossVersion.full),
  addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % betterMonadicForV),
  libraryDependencies ++= Seq(
    "javax.jms"         % "javax.jms-api"                  % jmsV,
    "org.typelevel"     %% "cats-core"                     % catsV,
    "org.typelevel"     %% "cats-effect"                   % catsEffectV,
    "co.fs2"            %% "fs2-core"                      % fs2V,
    "co.fs2"            %% "fs2-io"                        % fs2V,
    "io.chrisdavenport" %% "log4cats-slf4j"                % log4catsV,
    "com.codecommit"    %% "cats-effect-testing-scalatest" % catsEffectScalaTestV % Test
  )
)

// General Settings
inThisBuild(
  List(
    organization := "dev.fpinbo",
    developers := List(
      Developer("azanin", "Alessandro Zanin", "ale.zanin90@gmail.com", url("https://github.com/azanin")),
      Developer("al333z", "Alessandro Zoffoli", "alessandro.zoffoli@gmail.com", url("https://github.com/al333z")),
      Developer("r-tomassetti", "Renato Tomassetti", "r.tomas1989@gmail.com", url("https://github.com/r-tomassetti"))
    ),
    homepage := Some(url("https://github.com/fp-in-bo/jms4s")),
    licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
    pomIncludeRepository := { _ => false },
    scalacOptions in (Compile, doc) ++= Seq(
      "-groups",
      "-sourcepath",
      (baseDirectory in LocalRootProject).value.getAbsolutePath,
      "-doc-source-url",
      "https://github.com/fp-in-bo/jms4s/blob/v" + version.value + "€{FILE_PATH}.scala"
    )
  )
)

addCommandAlias("buildAll", ";clean;scalafmtAll;+test;mdoc")
