package com.videoapi

import slick.jdbc.PostgresProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object TestCheck extends App {
  // Replace with your database connection configuration
  val db = Database.forConfig("postgres")

  // Define a query to fetch the table names from the information schema
  val listTablesQuery = sql"""
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = 'public' -- or replace with your schema
""".as[String]

  // Run the query and get the results
  val resultFuture = db.run(listTablesQuery)

  // Wait for the result and print the list of tables
  val tables = Await.result(resultFuture, 10.seconds)
  println("Tables in the database:")
  tables.foreach(println)

  db.close()
}
