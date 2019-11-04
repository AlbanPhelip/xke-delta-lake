package fr.xebia.xke.deltalake.model

case class Customer(id: Int,
                    firstName: String,
                    lastName: String,
                    age: Int,
                    deleted: Boolean)
