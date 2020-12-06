package fr.xebia.xke.deltalake.model

case class CustomerV2(id: Int,
                      firstName: String,
                      lastName: String,
                      age: Int,
                      country: String,
                      deleted: Boolean)
