package fr.xebia.xke.deltalake.utils

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

object ExtensionMethodsUtils {

  implicit class DataFrameWriterOps[T](dfw: DataFrameWriter[T]) {
    def delta(path: String): Unit = {
      dfw.format("delta").save(path)
    }
  }

  implicit class DataFrameReaderOps(dfr: DataFrameReader) {
    def delta(path: String): DataFrame = {
      dfr.format("delta").load(path)
    }
  }

}
