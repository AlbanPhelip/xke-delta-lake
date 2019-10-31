package fr.xebia.xke.deltalake.readwrite

import fr.xebia.xke.deltalake.model.{People, Person}
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SaveMode}

case class Truc(i: Int)
object DeltaSchema extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPathParquet = s"$rootPath/person-schema-parquet"
  val personPathDelta = s"$rootPath/person-schema-delta"

  FileUtils.delete(personPathParquet)
  FileUtils.delete(personPathDelta)

  val dfPerson: DataFrame = List(
    Person("Toto", 21, "2019-10-01"),
    Person("Titi", 30, "2019-10-01")
  ).toDF()

  //val dfPeople = List(
  //  People("John", "Doe")
  //).toDF()

  val dfPeople = List(
    Truc(1)
  ).toDF()

  // Parquet
  dfPerson.write.mode(SaveMode.Append).parquet(personPathParquet)
  dfPeople.write.mode(SaveMode.Append).parquet(personPathParquet)

  spark.read.parquet(personPathParquet).show()

  // Delta
  dfPerson.write.mode(SaveMode.Append).delta(personPathDelta)
  dfPeople.write.mode(SaveMode.Append).delta(personPathDelta)

  spark.read.delta(personPathDelta).show()
}
