package fr.xebia.xke.deltalake

import fr.xebia.xke.deltalake.utils.SparkSessionProvider
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import org.apache.spark.sql.{DataFrame, SaveMode}
import io.delta.tables._

object DeltaLake extends App with SparkSessionProvider {

  import spark.implicits._

}
