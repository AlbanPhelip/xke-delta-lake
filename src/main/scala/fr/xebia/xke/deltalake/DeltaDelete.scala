package fr.xebia.xke.deltalake

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables._
import org.apache.spark.sql.{Dataset, Row, SaveMode}

object DeltaDelete extends App with SparkSessionProvider {

  import spark.implicits._

  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person"

  FileUtils.delete(personPath)

  val persons = List(
    Person("Toto", 21, "2019-10-01"),
    Person("Titi", 30, "2019-10-01")
  ).toDF()

  persons.write.mode(SaveMode.Overwrite).delta(personPath)

  // Create Delta Table
  val deltaPerson = DeltaTable.forPath(personPath)

  deltaPerson.delete($"name" === "Toto")

  println("Final parquet read")
  spark.read.parquet(personPath).show()
  println("Final delta read")
  spark.read.delta(personPath).show()

  deltaPerson.history().orderBy("version").show(truncate = false)


}
