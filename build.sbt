name := "sparkprofiler"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

enablePlugins(JavaAppPackaging)

val hadoopVersion = "2.6.0"

val sparkVersion = "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0"

// The dependency override for http://stackoverflow.com/questions/33815396/spark-com-fasterxml-jackson-module-error
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.2"

// The dependency override for javax.servelet is due to a conflict between Hadoop and Spark's
// respective dependencies on the library with differnet versions.
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided"

//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"

dependencyOverrides += "javax.servlet" % "javax.servlet-api" % "3.1.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % "provided"

