package com.videoapi.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.videoapi.repository.VideoUrlRepository
import com.videoapi.schema.VideoUrl
import spray.json.DefaultJsonProtocol.seqFormat

import scala.concurrent.ExecutionContext

// JSON format for marshalling/unmarshalling VideoUrl case class
object JsonFormats {
  import DefaultJsonProtocol._

  implicit val videoUrlFormat: RootJsonFormat[VideoUrl] = jsonFormat2(VideoUrl)
}

class VideoUrlRoutes(repository: VideoUrlRepository)(implicit ec: ExecutionContext) {

  import JsonFormats._

  val routes: Route = path("getUrls") {
    get {
      parameters("videoIds".as[String]) { videoIdsParam =>
        val videoIds = videoIdsParam.split(",").toList

        onSuccess(repository.getUrlsByVideoIds(videoIds)) { result =>
          complete(StatusCodes.OK, result)
        }
      }
    }
  }
}
