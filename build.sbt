import AssemblyKeys._

name := "IO Bench"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.2"

assemblySettings

jarName in assembly := "iobench.jar"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.2"

libraryDependencies += "junit" % "junit" % "4.10"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2-SNAPSHOT"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.2-SNAPSHOT"