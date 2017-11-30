name := "db-wrapper"

version := "0.1"

scalaVersion := "2.12.4"

resolvers += Classpaths.typesafeReleases

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.iq80.leveldb" % "leveldb" % "0.1",
  "org.scalaz" %% "scalaz-core" % "7.2.16",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)
