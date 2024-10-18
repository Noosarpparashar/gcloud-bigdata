package com.videoapi.repository

import com.videoapi.schema.VideoUrl
import com.videoapi.schema.VideoUrlRepository.videoUrls
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class VideoUrlRepository(db: Database)(implicit ec: ExecutionContext) {
  def getUrlsByVideoIds(videoIds: List[String]): Future[Seq[VideoUrl]] = {
    val query = videoUrls.filter(_.videoId inSet videoIds)
    println(s"Running query: $query with videoIds: $videoIds") // Log query details
    db.run(query.result)
  }
}
