package fr.xebia.xke.deltalake.update

import fr.xebia.xke.deltalake.model.Customer
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

object DeltaMerge extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-merge"

  FileUtils.delete(personPath)

  val customers: DataFrame = List(
    Customer(0, "Grace", "Hopper", 50, deleted = false),
    Customer(1, "Alan", "Turing", 38, deleted = false),
    Customer(2, "Margaret", "Hamilton", 41, deleted = false)
  ).toDF()

  customers.coalesce(3).write.mode(SaveMode.Overwrite).delta(personPath)

  val newCustomers: DataFrame = List(
    Customer(1, "Alan", "Turing", 38, deleted = true),
    Customer(2, "Margaret", "Hamilton", 42, deleted = false),
    Customer(3, "Linus", "Torvalds", 23, deleted = false)
  ).toDF()

  val deltaCustomer = DeltaTable.forPath(personPath)

  println("Before merge")
  deltaCustomer.toDF.show()

  println("New data")
  newCustomers.show()

  val oldTableName = "old-customer"
  val newTableName = "new-customer"
  val columnsUpdate = deltaCustomer.allColumns(newTableName)

  deltaCustomer.as(oldTableName)
    .merge(newCustomers.as(newTableName),  $"$oldTableName.customerId" ===  $"$newTableName.customerId")
    .whenMatched(col(s"$newTableName.deleted") === true)
    .delete()
    .whenMatched()
    .update(columnsUpdate)
    .whenNotMatched()
    .insert(columnsUpdate)
    .execute()

  println("After merge")
  spark.read.delta(personPath).show()

  deltaCustomer.history.orderBy("version").show(truncate = false)

}
