package fr.xebia.xke.deltalake.readwrite

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SaveMode}

object DeltaTimeTravel extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-time-travel"
  FileUtils.delete(personPath)

  val df1: DataFrame = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05")
  ).toDF()

  val df2: DataFrame = List(
    Person("Tata", 28, "2019-11-06"),
    Person("Tutu", 42, "2019-11-06")
  ).toDF()

  val saveMode = SaveMode.Append

  df1.write.mode(saveMode).delta(personPath)
  df2.write.mode(saveMode).delta(personPath)

  println("Delta table")
  spark.read.delta(personPath).show()

  println("Time travel")
  spark.read.option("versionAsOf", 0).delta(personPath).show()

}
