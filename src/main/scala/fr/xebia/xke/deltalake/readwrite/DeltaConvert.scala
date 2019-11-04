package fr.xebia.xke.deltalake.readwrite

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SaveMode}

object DeltaConvert extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-convert"

  FileUtils.delete(personPath)

  val dfPerson: DataFrame = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05")
  ).toDF()

  dfPerson.write.mode(SaveMode.Append).parquet(personPath)

  val deltaTable = DeltaTable.convertToDelta(spark, s"parquet.`$personPath`", "date string")

  deltaTable.history().show(truncate = false)
}
