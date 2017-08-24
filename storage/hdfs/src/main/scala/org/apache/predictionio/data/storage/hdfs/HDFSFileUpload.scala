package org.apache.predictionio.data.storage.hdfs

import java.io.{File, FileInputStream}

import org.apache.hadoop.fs.Path
import org.apache.predictionio.data.storage.{FileUpload, StorageClientConfig}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by xie on 17/8/23.
  */
class HDFSFileUpload(
  clientMap: Map[String,AnyRef],
  config: StorageClientConfig,
  prefix: String) extends FileUpload{

  private final val hdfsRoot = clientMap("RootPath").asInstanceOf[String]
  private final val fs = clientMap("HDFSClient").asInstanceOf[org.apache.hadoop.fs.FileSystem]

  def futureUpload(localPath: String, appId: Int, channelId: Int)(implicit ec: ExecutionContext):
  Future[String] = Future{
    val fileName = localPath.split("/").last
    val basePath = s"${hdfsRoot}/app_${appId}/upload"
    fs.mkdirs(new Path(basePath))
    val hdfsPath = basePath + "/" + fileName
    fs.copyFromLocalFile(new Path(localPath),new Path(hdfsPath))
    hdfsPath
  }

  def upload(localPath: String, appId: Int, channelId: Int) = {
    val fileName = localPath.split("/").last
    val basePath = s"${hdfsRoot}/app_${appId}/upload"
    fs.mkdirs(new Path(basePath))
    val hdfsPath = basePath + "/" + fileName
    fs.copyFromLocalFile(new Path(localPath),new Path(hdfsPath))
    hdfsPath
  }
}
