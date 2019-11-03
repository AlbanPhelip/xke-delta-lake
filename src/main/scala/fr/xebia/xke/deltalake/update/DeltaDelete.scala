package fr.xebia.xke.deltalake.update

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object DeltaDelete extends App with SparkSessionProvider {

  import spark.implicits._

  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person-delete"
  FileUtils.delete(personPath)

  val persons = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05"),
    Person("Titi", 30, "2019-11-05"),

    Person("Titi", 30, "2019-11-05"),
    Person("Toto", 30, "2019-11-05"),
    Person("Tata", 30, "2019-11-05"),
  ).toDF()

  persons.coalesce(2).write.mode(SaveMode.Overwrite).delta(personPath)

  // Create Delta Table
  val deltaPerson = DeltaTable.forPath(personPath)

  deltaPerson.delete(col("name") === "Toto")
  deltaPerson.delete(col("name") === "Tata")

  println("Final delta read")
  spark.read.delta(personPath).show()
  println("Final parquet read")
  spark.read.parquet(personPath).show()

  println("History")
  deltaPerson.history().show(truncate = false)


}
