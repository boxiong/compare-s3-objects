name := "SkygateSparkApps"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.rogach" %% "scallop" % "3.3.0",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "com.amazonaws" % "aws-java-sdk" % "1.11.563"
)
