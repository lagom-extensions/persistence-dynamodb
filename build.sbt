import sbt.Keys.libraryDependencies
import sbt.file

organization := "com.lightbend.lagom.dynamodb"
homepage := Some(
  url("https://github.com/lagom-extensions/persistence-dynamodb")
)
scmInfo := Some(
  ScmInfo(
    url("https://github.com/lagom-extensions/persistence-dynamodb.git"),
    "git@github.com:lagom-extensions/persistence-dynamodb.git"
  )
)
developers := List(
  Developer(
    "Dmitriy Kuzkin",
    "Dmitriy Kuzkin",
    "dmitriy.kuzkin@gmail.com",
    url("https://github.com/kuzkdmy")
  )
)
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslServer,
      lagomScaladslPersistence,
      lagomScaladslDevMode,
      lagomScaladslTestKit,
      "com.softwaremill.macwire" %% "macros"                          % "2.2.5" % "provided",
      "com.gu"                   %% "scanamo-alpakka"                 % "1.0.0-M7",
      "com.amazonaws"            % "dynamodb-streams-kinesis-adapter" % "1.4.0",
      "com.typesafe.akka"        %% "akka-persistence-dynamodb"       % "1.1.1",
      "org.slf4j"                % "slf4j-api"                        % "1.7.25",
      "org.slf4j"                % "slf4j-simple"                     % "1.7.25",
      "org.scalatest"            %% "scalatest"                       % "3.0.0" % "test"
    )
  )

testOptions in Test += Tests.Setup(() => {
  System.setProperty("aws.accessKeyId", "dummyAccessKeyId")
  System.setProperty("aws.secretKey", "dummySecretKey")
})
