name := "xke-delta-lake"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"
val deltaLakeVersion = "0.7.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "io.delta" %% "delta-core" % deltaLakeVersion