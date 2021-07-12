val Scala213 = "2.13.5"
val Scala212 = "2.12.14"
val Java18   = "adopt@1.8"
val Java11   = "adopt@1.11"

enablePlugins(SonatypeCiReleasePlugin)
Global / onChangedBuildSource := ReloadOnSourceChanges
ThisBuild / mimaFailOnProblem := false
ThisBuild / mimaFailOnNoPrevious := false

ThisBuild / scalaVersion := Scala213
ThisBuild / crossScalaVersions := Seq(Scala213, Scala212)
ThisBuild / organization := "dev.fpinbo"
ThisBuild / organizationName := "Functional Programming in Bologna"
ThisBuild / publishFullName := "Alessandro Zoffoli"
ThisBuild / publishGithubUser := "al333z"
ThisBuild / githubWorkflowJavaVersions := Seq(Java18, Java11)
ThisBuild / baseVersion := "0.0.1"

//CI definition
val MicrositesCond = s"matrix.scala == '$Scala212'"

def micrositeWorkflowSteps(cond: Option[String] = None): List[WorkflowStep] = List(
  WorkflowStep.Use(
    UseRef.Public("ruby", "setup-ruby", "v1"),
    params = Map("ruby-version" -> "2.6"),
    cond = cond
  ),
  WorkflowStep.Run(List("gem update --system"), cond = cond),
  WorkflowStep.Run(List("gem install sass"), cond = cond),
  WorkflowStep.Run(List("gem install jekyll -v 4"), cond = cond)
)

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep
    .Run(List("docker-compose up --renew-anon-volumes --force-recreate -d"), name = Some("Start docker containers")),
  WorkflowStep.Sbt(List("test"), name = Some("Test")),
  WorkflowStep.Run(List("docker-compose down"), name = Some("Stop docker containers"))
//  WorkflowStep.Sbt(List("mimaReportBinaryIssues"), name = Some("Binary Compatibility Check"))
) ++ micrositeWorkflowSteps(Some(MicrositesCond)).toSeq :+ WorkflowStep.Sbt(
  List("site/makeMicrosite"),
  cond = Some(MicrositesCond)
)

ThisBuild / githubWorkflowAddedJobs ++= Seq(
  WorkflowJob(
    "scalafmt",
    "Scalafmt",
    githubWorkflowJobSetup.value.toList ::: List(
      WorkflowStep.Sbt(List("scalafmtCheckAll"), name = Some("Scalafmt"))
    ),
    // Awaiting release of https://github.com/scalameta/scalafmt/pull/2324/files
    scalas = crossScalaVersions.value.toList.filter(_.startsWith("2."))
  ),
  /*  WorkflowJob(
    "microsite",
    "Microsite",
    githubWorkflowJobSetup.value.toList ::: (micrositeWorkflowSteps(None) :+ WorkflowStep
      .Sbt(List("site/makeMicrosite"), name = Some("Build the microsite"))),
    scalas = List(Scala212)
  ),*/
  WorkflowJob( //This step is to collect the entire build outcome since mergify is not acting properly with githubactions.
    id = "build-success",
    name = "Build Success",
    needs = List("build", "scalafmt"),
    steps = List(WorkflowStep.Run(List("echo Build Succeded"))),
    oses = List("ubuntu-latest"),
    //These are useless but we don't know how to remove the scalas and javas attributes
    // (if you provide empty list it will create an empty list in the yml which is wrong)
    scalas = List(Scala213),
    javas = List(Java18)
  )
)

ThisBuild / githubWorkflowTargetBranches := List("*")
ThisBuild / githubWorkflowTargetTags ++= Seq("v*")
ThisBuild / githubWorkflowPublishTargetBranches := Seq(RefPredicate.StartsWith(Ref.Tag("v")))

ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    List("release")
  )
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "site",
  name = "Deploy site",
  needs = List("build"),
  javas = List(Java11),
  scalas = List(Scala213),
  cond = """
           | always() &&
           | needs.build.result == 'success' &&
           | (github.ref == 'refs/heads/main')
  """.stripMargin.trim.linesIterator.mkString.some,
  steps = githubWorkflowGeneratedDownloadSteps.value.toList :+
    WorkflowStep.Use(
      UseRef.Public("peaceiris", "actions-gh-pages", "v3"),
      name = Some(s"Deploy site"),
      params = Map(
        "publish_dir"  -> "./site/target/site",
        "github_token" -> "${{ secrets.GITHUB_TOKEN }}"
      )
    )
)

val catsV                = "2.6.1"
val jmsV                 = "2.0.1"
val ibmMQV               = "9.2.2.0"
val activeMQV            = "2.17.0"
val catsEffectV          = "3.1.1"
val catsEffectScalaTestV = "1.1.1"
val fs2V                 = "3.0.6"
val log4catsV            = "2.1.1"
val log4jSlf4jImplV      = "2.14.1"

val kindProjectorV    = "0.13.0"
val betterMonadicForV = "0.3.1"

// Projects
lazy val jms4s = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, ibmMQ, activeMQArtemis, tests, examples, site)
  .settings(commonSettings, releaseSettings)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings, releaseSettings)
  .settings(name := "jms4s")
  .settings(Test / parallelExecution := false)

lazy val ibmMQ = project
  .in(file("ibm-mq"))
  .settings(commonSettings, releaseSettings)
  .settings(name := "jms4s-ibm-mq")
  .settings(libraryDependencies += "com.ibm.mq" % "com.ibm.mq.allclient" % ibmMQV)
  .settings(Test / parallelExecution := false)
  .dependsOn(core)

lazy val activeMQArtemis = project
  .in(file("active-mq-artemis"))
  .settings(commonSettings, releaseSettings)
  .settings(name := "jms4s-active-mq-artemis")
  .settings(libraryDependencies += "org.apache.activemq" % "artemis-jms-client-all" % activeMQV)
  .settings(Test / parallelExecution := false)
  .dependsOn(core)

lazy val tests = project
  .in(file("tests"))
  .settings(commonSettings, releaseSettings)
  .enablePlugins(NoPublishPlugin)
  .settings(libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jSlf4jImplV % Runtime)
  .settings(Test / parallelExecution := false)
  .dependsOn(ibmMQ, activeMQArtemis)

lazy val examples = project
  .in(file("examples"))
  .settings(commonSettings, releaseSettings)
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
      scalacOptions --= Seq(
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
  scalafmtOnCompile := true,
  addCompilerPlugin("org.typelevel" %% "kind-projector"     % kindProjectorV cross CrossVersion.full),
  addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % betterMonadicForV),
  libraryDependencies ++= Seq(
    "javax.jms"     % "javax.jms-api"                  % jmsV,
    "org.typelevel" %% "cats-core"                     % catsV,
    "org.typelevel" %% "cats-effect"                   % catsEffectV,
    "co.fs2"        %% "fs2-core"                      % fs2V,
    "org.typelevel" %% "log4cats-slf4j"                % log4catsV,
    "org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectScalaTestV % Test
  )
)

lazy val releaseSettings = {
  Seq(
    Test / publishArtifact := false,
    homepage := Some(url("https://github.com/fp-in-bo/jms4s")),
    startYear := Some(2020),
    licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/fp-in-bo/jms4s"),
        "git@github.com:fp-in-bo/jms4s.git"
      )
    ),
    developers := List(
      Developer("azanin", "Alessandro Zanin", "ale.zanin90@gmail.com", url("https://github.com/azanin")),
      Developer("al333z", "Alessandro Zoffoli", "alessandro.zoffoli@gmail.com", url("https://github.com/al333z")),
      Developer("faustin0", "Fausto Di Natale", "dinatalefausto@gmail.com", url("https://github.com/faustin0")),
      Developer("r-tomassetti", "Renato Tomassetti", "r.tomas1989@gmail.com", url("https://github.com/r-tomassetti"))
    )
  )
}

addCommandAlias("buildAll", ";clean;scalafmtAll;+test;mdoc")
