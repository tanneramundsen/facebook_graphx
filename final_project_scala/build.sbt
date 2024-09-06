name := "facebook_graph"

version := "1.0.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.5.1" % "provided",
  "org.apache.spark" % "spark-sql_2.12" % "3.5.1" % "provided",
  "org.apache.spark" % "spark-graphx_2.12" % "3.5.1" % "provided"

)