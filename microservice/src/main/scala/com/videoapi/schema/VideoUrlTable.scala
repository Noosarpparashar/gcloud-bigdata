package com.videoapi.schema

import slick.jdbc.PostgresProfile.api._

case class VideoUrl(videoId: String, url: String)

class VideoUrlTable(tag: Tag) extends Table[VideoUrl](tag, "yvideo_api_url") {
  def videoId = column[String]("video_id", O.PrimaryKey)
  def url = column[String]("url")

  def * = (videoId, url) <> ((VideoUrl.apply _).tupled, VideoUrl.unapply)
}

object VideoUrlRepository {
  val videoUrls = TableQuery[VideoUrlTable]
}
