package fr.xebia.xke.deltalake.utils

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
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

  implicit class DataStreamWriterOps[T](dfsw: DataStreamWriter[T]) {
    def delta(path: String): StreamingQuery = {
      dfsw.format("delta").start(path)
    }
  }

  implicit class DataStreamReaderOps(dsr: DataStreamReader) {
    def delta(path: String): DataFrame = {
      dsr.format("delta").load(path)
    }
  }
}
