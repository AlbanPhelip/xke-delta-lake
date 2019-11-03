package fr.xebia.xke.deltalake.readwrite

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import org.apache.spark.sql.{DataFrame, SaveMode}

object DeltaReadWrite extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head

  val df: DataFrame = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05")
  ).toDF()

  def runReadAndWrite(saveMode: SaveMode): Unit = {
    val personPath = s"$rootPath/person-$saveMode"
    FileUtils.delete(personPath)

    df.write.mode(saveMode).delta(personPath)
    Thread.sleep(3000)
    df.write.mode(saveMode).delta(personPath)

    println(s"----- $saveMode -----")
    println(s"Delta read with $saveMode mode")
    spark.read.delta(personPath).show()

    println(s"Parquet read with $saveMode mode")
    spark.read.parquet(personPath).show()
  }

  runReadAndWrite(SaveMode.Append)
  runReadAndWrite(SaveMode.Overwrite)
}
