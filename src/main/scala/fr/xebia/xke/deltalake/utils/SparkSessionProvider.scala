package fr.xebia.xke.deltalake.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Delta Lake XKE")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
