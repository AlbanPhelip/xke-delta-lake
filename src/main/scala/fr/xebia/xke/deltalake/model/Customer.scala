package fr.xebia.xke.deltalake.model

case class Customer(customerId: Int,
                    firstName: String,
                    lastName: String,
                    age: Int,
                    deleted: Boolean)
