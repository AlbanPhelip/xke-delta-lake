package fr.xebia.xke.deltalake

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables._
import org.apache.spark.sql.SaveMode

object DeltaLake extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person"

  FileUtils.delete(personPath)

  val persons = List(
    Person("Toto", 21, "2019-10-01"),
    Person("Titi", 30, "2019-10-01")
  ).toDF()

  persons.write.mode(SaveMode.Overwrite).delta(personPath)

  val newPerson = List(
    Person("Toto", 22, "2019-10-09"),
    Person("Tata", 51, "2019-10-09")
  ).toDF()

  val deltaPerson = DeltaTable.forPath(personPath)

  println("Before merge")
  deltaPerson.toDF.show()

  val oldTableName = "oldPerson"
  val newTableName = "newPerson"
  val columnsUpdate = deltaPerson.allColumns(newTableName)

  deltaPerson.as(oldTableName)
    .merge(newPerson.as(newTableName),  $"$oldTableName.name" ===  $"$newTableName.name")
    .whenMatched()
    .update(columnsUpdate)
    .whenNotMatched
    .insert(columnsUpdate)
    .execute()

  println("After merge")
  deltaPerson.toDF.show()
  spark.read.delta(personPath).show()

  deltaPerson.history.orderBy("version").show(truncate = false)

}
