package fr.xebia.xke.deltalake

import fr.xebia.xke.deltalake.utils.SparkSessionProvider
import org.apache.spark.sql.DataFrame

object DeltaLake extends App with SparkSessionProvider {

  import spark.implicits._

  val df: DataFrame = List(1, 2, 3).toDF("col1")

  df.show()

  df.write.format("delta").save(args.head)

}
