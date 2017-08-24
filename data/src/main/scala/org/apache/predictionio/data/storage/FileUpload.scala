package org.apache.predictionio.data.storage

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by xie on 17/8/23.
  */
trait FileUpload {

  /**
    * upload file in @localPath to FileStorage according to appId and channelId
    * @param localPath
    * @param appId
    * @param channelId
    * @return upload file path
    */
  def futureUpload(localPath: String, appId: Int, channelId: Int)(implicit ec: ExecutionContext): Future[String]

  def upload(localPath: String, appId: Int, channelId: Int): String

}
