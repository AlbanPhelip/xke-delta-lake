package fr.xebia.xke.deltalake.concurrency

import fr.xebia.xke.deltalake.utils.{FileUtils, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import fr.xebia.xke.deltalake.utils.ExtensionMethodsUtils._
import io.delta.tables.DeltaTable


object ThreadTest extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-concurrency"

  FileUtils.delete(personPath)

  val customers: DataFrame = List(
    (0, "Grace", "Hopper", 50, "2020-12-07"),
    (1, "Alan", "Turing", 38, "2020-12-08")
  ).toDF("id", "first_name", "last_name", "age", "date")

  customers.coalesce(3).write.partitionBy("date").mode(SaveMode.Overwrite).delta(personPath)

  val deltaCustomer = DeltaTable.forPath(personPath)

  def write1(): Unit = {
    for (i <- 1 to 10) {
      println(s"Before merge1 #$i")
      deltaCustomer.update($"date" === "2020-12-08" and $"id" === 1, Map("age" -> $"age".+(1)))

      println(s"After merge #$i")
      spark.read.delta(personPath).show()
    }
  }

  def write2(): Unit = {
    for (i <- 1 to 10) {
      val newCustomers: DataFrame = List(
        (0, "Grace", "Hopper", 50 + i, "2020-12-07")
      ).toDF("id", "first_name", "last_name", "age", "date")

      println(s"Before merge2 #$i")
      deltaCustomer.as("old-customer")
        .merge(newCustomers.as("new-customer"), $"old-customer.id" === $"new-customer.id" and $"old-customer.date" === $"new-customer.date")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()

      println(s"After merge #$i")
      spark.read.delta(personPath).show()
    }
  }

  def writes = List(write1(), write2())

  val tasks: Seq[Future[Unit]] = for (i <- 1 to 2) yield Future {
    writes(i)
  }

  val aggregated: Future[Seq[Unit]] = Future.sequence(tasks)
  Await.result(aggregated, 60.seconds)

}
