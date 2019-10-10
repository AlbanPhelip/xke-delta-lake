package fr.xebia.xke.deltalake.model

case class Transaction(transactionId: Long,
                       accountId: Int,
                       amount: Double)
