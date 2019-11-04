package fr.xebia.xke.deltalake.streaming

import fr.xebia.xke.deltalake.model.Person
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}


object DeltaSourceBatch extends App with SparkSessionProvider {

  import spark.implicits._

  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person-source"
  FileUtils.delete(personPath)

  val df: DataFrame = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05")
  ).toDF()

  df.write.delta(personPath)

  for (i <- 1 to 100) {
    println(s"Appending #$i")
    DeltaTable.forPath(personPath).update(col("name") === "Titi", Map("age" -> lit(i)))
    Thread.sleep(1000)
  }

}


object DeltaSourceStream extends App with SparkSessionProvider {
  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person-source"

  val streamDF: DataFrame = spark.readStream.option("ignoreChanges", "true").delta(personPath)
  val query: StreamingQuery = streamDF.writeStream.format("console").outputMode(OutputMode.Append).start()

  query.awaitTermination()
}
