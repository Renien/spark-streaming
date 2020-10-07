name := "spark-streamingd"

version := "4.0.0"

scalaVersion := "2.11.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
resolvers += "Hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "com.uber" % "h3" % "3.6.0",
  "org.apache.spark" % "spark-core_2.11" % "2.4.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"
)