package fr.xebia.xke.deltalake.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col

object DeltaUtils {

  def getFirstTimestamp(deltaPath: String): String = {
    DeltaTable.forPath(deltaPath)
      .history
      .filter(col("version") === 0)
      .first
      .getAs[java.sql.Timestamp]("timestamp")
      .toString
  }

}
