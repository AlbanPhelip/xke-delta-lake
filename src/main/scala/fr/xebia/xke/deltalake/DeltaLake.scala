package fr.xebia.xke.deltalake

import fr.xebia.xke.deltalake.utils.SparkSessionProvider
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import org.apache.spark.sql.{DataFrame, SaveMode}

object DeltaLake extends App with SparkSessionProvider {

  import spark.implicits._

  val df: DataFrame =  (1 to 100000).toDF("col1").coalesce(1).cache()
  df.count()

  val rootPath = args.head
  val delta = s"$rootPath/table_delta"
  val parquet = s"$rootPath/table_parquet"
  val saveMode = SaveMode.Append


  val t3: Long = System.currentTimeMillis()
  df.write.mode(saveMode).parquet(parquet)
  val t4: Long = System.currentTimeMillis()

  val t1: Long = System.currentTimeMillis()
  df.write.mode(saveMode).delta(delta)
  val t2: Long = System.currentTimeMillis()

  val t5: Long = System.currentTimeMillis()
  spark.read.delta(delta).count
  val t6: Long = System.currentTimeMillis()

  val t7: Long = System.currentTimeMillis()
  spark.read.parquet(parquet).count
  val t8: Long = System.currentTimeMillis()

  println(s"Time writing delta: ${t2-t1} ms")
  println(s"Time writing parquet: ${t4-t3} ms")
  println(s"Time reading delta: ${t6-t5} ms")
  println(s"Time reading parquet: ${t8-t7} ms")

}
