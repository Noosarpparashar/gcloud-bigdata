package com.videoapi

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.videoapi.repository.VideoUrlRepository
import com.videoapi.routes.VideoUrlRoutes

import scala.concurrent.ExecutionContextExecutor
import slick.jdbc.PostgresProfile.api._

object VideoApiApp extends App {

  implicit val system: ActorSystem = ActorSystem("videoApiSystem")
  //implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  // Initialize the PostgreSQL database connection
  val db = Database.forConfig("postgres")

  // Initialize repository
  val videoUrlRepository = new VideoUrlRepository(db)

  // Initialize routes
  val routes = new VideoUrlRoutes(videoUrlRepository).routes

  // Bind and handle HTTP requests
  Http().newServerAt("localhost", 8080).bind(routes)

  println("Server started at http://localhost:8080/")
}
