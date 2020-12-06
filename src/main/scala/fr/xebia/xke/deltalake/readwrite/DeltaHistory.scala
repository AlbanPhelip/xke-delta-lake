package fr.xebia.xke.deltalake.readwrite

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SaveMode}
import io.delta.tables.DeltaTable

object DeltaHistory extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-history"
  FileUtils.delete(personPath)

  val df1: DataFrame = List(
    Person("Toto", 21, "2020-12-07"),
    Person("Titi", 30, "2020-12-07")
  ).toDF()

  val df2: DataFrame = List(
    Person("Toto", 21, "2020-12-07"),
    Person("Titi", 30, "2020-12-07"),
    Person("Tata", 42, "2020-12-07")
  ).toDF()

  df1.coalesce(1).write.mode(SaveMode.Append).option("userMetadata", "Initial write").delta(personPath)
  df2.coalesce(2).write.mode(SaveMode.Append).option("userMetadata", "Second write").delta(personPath)
  df2.coalesce(3).write.mode(SaveMode.Append).option("userMetadata", "Third write").delta(personPath)

  val history: DataFrame = DeltaTable.forPath(personPath).history()

  history.show(truncate = false)
}
