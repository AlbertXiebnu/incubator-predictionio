package org.apache.predictionio.data.api

import java.io.File

import org.apache.predictionio.data.storage.Storage
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.HttpChallenge
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.AuthenticationFailedRejection.{CredentialsMissing, CredentialsRejected}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive1}
import akka.http.scaladsl.server.directives.AuthenticationDirective
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by xie on 17/8/22.
  */
case class FileUploadConfig(
   ip: String = "localhost",
   port: Int = 7070,
   maxFileSize: Int = 1024)

object FileUploadServer extends JsonSupport{
  def createFileUploadServer(config: FileUploadConfig): ActorSystem = {
    implicit val system = ActorSystem("FileUploadServerSystem")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val fileClient = Storage.getFileUpload()
    val accessKeyClient = Storage.getMetaDataAccessKeys()
    val channelsClient = Storage.getMetaDataChannels()

    val maxFileSizeBytes = config.maxFileSize * 1024 * 1024

    case class AuthData(appId: Int, channelId: Option[Int], events: Seq[String])
    case class AuthParam(accessKey: Option[String],channel: Option[String])

    val FailedAuth = Left(HttpChallenge("OAuth2",None))

    val storagePath = new File("tmp")
    if(!storagePath.exists()) storagePath.mkdirs()

    def authenticated[T](authenticator: AuthParam => Future[AuthenticationResult[T]]):AuthenticationDirective[T] = {
      parameters('accessKey.?,'channel.?).tflatMap{
        case (accessKey,channel) =>
          onSuccess(authenticator(AuthParam(accessKey,channel))).flatMap{
            case Right(s) => provide(s)
            case Left(challenge) =>
              reject(AuthenticationFailedRejection(CredentialsRejected,challenge)): Directive1[T]
          }
        case _ =>
          reject(AuthenticationFailedRejection(CredentialsMissing, HttpChallenge("OAuth2",None))): Directive1[T]
      }
    }

    def withAccessKey: AuthParam => Future[AuthenticationResult[AuthData]] = {
      authParam: AuthParam =>
        Future{
          val accessKeyParamOpt = authParam.accessKey
          val channelParamOpt = authParam.channel
          accessKeyParamOpt.map{ accessKeyParam =>
            accessKeyClient.get(accessKeyParam).map{ k =>
              channelParamOpt.map{ ch =>
                val channelMap =
                  channelsClient.getByAppid(k.appid)
                    .map(c => (c.name,c.id)).toMap
                if(channelMap.contains(ch)){
                  Right(AuthData(k.appid,Some(channelMap(ch)),k.events))
                }else{
                  FailedAuth
                }
              }.getOrElse{
                Right(AuthData(k.appid,None,k.events))
              }
            }.getOrElse(FailedAuth)
          }.getOrElse(FailedAuth)
        }
    }

    val route = handleExceptions(FileUploadCommon.exceptionHandler){
      handleRejections(FileUploadCommon.rejectionHandler){
        withSizeLimit(maxFileSizeBytes) {
          (path("upload") & authenticated(withAccessKey)) { authData =>
            fileUpload("file") {
              case (metadata, byteSource) =>
                val appId = authData.appId
                val channelId = authData.channelId.getOrElse(0)
                val f = new File(storagePath.toString + s"/${metadata.fileName}")
                val futureDone: Future[IOResult] = byteSource.runWith(FileIO.toPath(f.toPath))
                val futurePath = futureDone.map{ result =>
                  val hdfsPath = fileClient.upload(f.getCanonicalPath,appId,channelId)
                  hdfsPath
                }
                onComplete(futurePath){ savePath =>
                  complete(FileUploadResult(StatusCodes.OK.value, savePath.get, f.length.toInt))
                }
            }
          }
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(route,config.ip,config.port)
    system
  }
}
