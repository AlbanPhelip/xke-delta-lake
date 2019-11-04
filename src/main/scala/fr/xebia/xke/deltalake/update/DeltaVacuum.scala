package fr.xebia.xke.deltalake.update

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit}

object DeltaVacuum extends App with SparkSessionProvider {

  import spark.implicits._

  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person-vacuum"

  FileUtils.delete(personPath)

  val persons = List(
    Person("Titi", 21, "2019-11-05"),
    Person("Tztz", 30, "2019-11-05"),
  ).toDF()

  persons.write.mode(SaveMode.Overwrite).delta(personPath)

  // Create Delta Table
  val deltaPerson = DeltaTable.forPath(personPath)

  deltaPerson.update(col("name") === "Tztz", Map("name" -> lit("Tata")))

  // Print tables before vaccum
  println("Parquet read")
  spark.read.parquet(personPath).show()
  println("Delta read")
  spark.read.delta(personPath).show()

  // Vaccum
  deltaPerson.vacuum(0d)

  // Print tables after vacuum
  println("Parquet read")
  spark.read.parquet(personPath).show()
  println("Delta read")
  spark.read.delta(personPath).show()

}
