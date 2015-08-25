name := "spark-mainframe-connector"

version := "1.0.0"

organization := "com.syncsort"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "commons-net" % "commons-net" % "3.1"

pomExtra := (
  <url>https://github.com/syncsort/spark-mainframe</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <developers>
    <developer>
     <id>masokan</id>
      <name>M. Asokan</name>
      <url>mailto:masokan@syncsort.com</url>
    </developer>
  </developers>
)


libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.mockito" % "mockito-all" % "2.0.2-beta"

libraryDependencies += "org.testng" % "testng" % "6.1.1"


