name := "xke-delta-lake"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.3"
val deltaLakeVersion = "0.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "io.delta" %% "delta-core" % deltaLakeVersion