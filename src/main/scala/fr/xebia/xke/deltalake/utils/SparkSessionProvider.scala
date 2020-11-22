package fr.xebia.xke.deltalake.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Delta Lake XKE")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
