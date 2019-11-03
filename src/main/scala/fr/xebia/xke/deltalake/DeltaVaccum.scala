package fr.xebia.xke.deltalake

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables._
import org.apache.spark.sql.SaveMode

object DeltaVaccum extends App with SparkSessionProvider {

  import spark.implicits._

  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person"

  FileUtils.delete(personPath)

  val persons = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05")
  ).toDF()

  persons.write.mode(SaveMode.Overwrite).delta(personPath)

  val deltaPerson = DeltaTable.forPath(personPath)

  deltaPerson.vacuum().show()

  deltaPerson.history().orderBy("version").show(truncate = false)


}
