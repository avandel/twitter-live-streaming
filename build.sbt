name := "twitter"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" ,
  "org.apache.spark" %% "spark-sql" % "2.1.1" ,
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.1",
  "org.scalaj" %% "scalaj-http" % "2.4.0",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyJarName in assembly := "spark-streaming-twitter.jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") =>
    MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}