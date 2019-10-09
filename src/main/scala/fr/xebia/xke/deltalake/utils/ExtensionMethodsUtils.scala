package fr.xebia.xke.deltalake.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, DataFrameWriter}
import org.apache.spark.sql.functions.col

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

  implicit class DeltaTableOps(detaTable: DeltaTable) {
    def allColumns(newTableName: String): Map[String, Column] = {
      detaTable
        .toDF
        .columns
        .map(column => column -> col(s"$newTableName.$column"))
        .toMap
    }
  }

}
