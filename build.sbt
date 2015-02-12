name := "expectFantasy"

version := "1.0"

scalaVersion := "2.11.4"

mainClass in Compile := Some("org.bustos.realityball.ExpectFantasy")

libraryDependencies ++= List(
  "com.typesafe.akka"   %% "akka-actor"    % "2.3.6",
  "com.typesafe.slick"  %% "slick"         % "2.1.0",
  "org.seleniumhq.selenium" % "selenium-java" % "2.35.0",
  "org.scalatest"      %% "scalatest" % "2.1.6",
  "io.spray"           %% "spray-can"     % "1.3.1",
  "io.spray"           %% "spray-routing" % "1.3.1",
  "io.spray"           %% "spray-json"    % "1.3.1",
  "mysql"               % "mysql-connector-java" % "latest.release",
  "log4j"               % "log4j"         % "1.2.14",
  "org.slf4j"           % "slf4j-api"     % "1.7.6",
  "org.slf4j"           % "slf4j-log4j12" % "1.7.6",
  "joda-time"           % "joda-time"     % "2.7",
  "org.joda"            % "joda-convert"  % "1.2"
)
