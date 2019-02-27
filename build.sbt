import scala.util.Properties.envOrElse

lazy val commonSettings = Seq(
   organization := "org.bustos",
   version := "0.1.0",
   scalaVersion := "2.11.7"
)

lazy val commons = {
  if (envOrElse("BUILD_ENV", "") == "") ProjectRef(uri("https://github.com/RealityBall/common.git"), "common")
  else ProjectRef(file("../common"), "common")
}

lazy val expectFantasy = (project in file("."))
   .settings(name := "expect-fantasy")
   .settings(commonSettings: _*)
   .settings(libraryDependencies ++= projectLibraries)
   .dependsOn(commons)

val slf4jV = "1.7.6"
val akka_http_version = "10.0.11"

val projectLibraries = Seq(
  "com.typesafe.slick"   %%  "slick"                  % "3.2.1",
  "com.typesafe.slick"   %%  "slick-hikaricp"         % "3.2.1",
  "com.github.tototoshi" %%  "scala-csv"              % "1.1.2",
  "log4j"                %   "log4j"                  % "1.2.14",
  "org.slf4j"            %   "slf4j-api"              % slf4jV,
  "org.slf4j"            %   "slf4j-log4j12"          % slf4jV,
  "org.scalatest"        %%  "scalatest"              % "2.1.6" % "test",
  "com.typesafe.akka"    %%  "akka-http-spray-json"   % akka_http_version,
  "mysql"                %   "mysql-connector-java"   % "latest.release",
  "joda-time"            %   "joda-time"              % "2.7",
  "org.joda"             %   "joda-convert"           % "1.2"
)

