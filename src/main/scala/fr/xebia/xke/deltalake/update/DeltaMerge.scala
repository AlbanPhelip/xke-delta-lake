package fr.xebia.xke.deltalake.update

import fr.xebia.xke.deltalake.model.{CustomerV1, CustomerV2}
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode}

object DeltaMerge extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-merge"

  FileUtils.delete(personPath)

  val customers: DataFrame = List(
    CustomerV1(0, "Grace", "Hopper", 50, deleted = false),
    CustomerV1(1, "Alan", "Turing", 38, deleted = false),
    CustomerV1(2, "Margaret", "Hamilton", 41, deleted = false)
  ).toDF()

  customers.coalesce(3).write.mode(SaveMode.Overwrite).delta(personPath)

  val newCustomers: DataFrame = List(
    CustomerV2(1, "Alan", "Turing", 38, "UK", deleted = true),
    CustomerV2(2, "Margaret", "Hamilton", 42, "USA", deleted = false),
    CustomerV2(3, "Linus", "Torvalds", 23, "Finland", deleted = false)
  ).toDF()

  val deltaCustomer = DeltaTable.forPath(personPath)

  println("Before merge")
  deltaCustomer.toDF.show()

  println("New data")
  newCustomers.show()

  spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

  deltaCustomer.as("old-customer")
    .merge(newCustomers.as("new-customer"), $"old-customer.id" === $"new-customer.id")
    .whenMatched($"new-customer.deleted" === true)
    .delete()
    .whenMatched()
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .execute()

  println("After merge")
  spark.read.delta(personPath).show()

  println("History")
  deltaCustomer.history.show(truncate = false)

  deltaCustomer.generate("symlink_format_manifest")
}
