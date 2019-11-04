package fr.xebia.xke.deltalake.streaming

import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}

object DeltaSink extends App with SparkSessionProvider {

  // Setup
  val rootPath = args.head
  val personPath = s"$rootPath/person-sink"
  val checkPointPath = s"$personPath-checkpoint"
  FileUtils.delete(personPath)
  FileUtils.delete(checkPointPath)


  val streamDF = spark.readStream.format("rate").load()
  val query = streamDF.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkPointPath)
    .delta(personPath)

  query.awaitTermination()

}