package org.apache.predictionio.data.api

/**
  * Created by xie on 17/8/22.
  */
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.predictionio.data.storage.StorageException
import spray.json.DefaultJsonProtocol

/**
  * Created by xie on 17/8/21.
  */

case class FileUploadResult(statusCode: String,path: String,size: Int)
case class HandlerResult(statusCode: String,msg: String)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val fileUploadFormat = jsonFormat3(FileUploadResult)
  implicit val handlerFormat = jsonFormat2(HandlerResult)
}

object FileUploadCommon extends JsonSupport{

  val rejectionHandler =
    RejectionHandler.newBuilder()
      .handle{ case MalformedRequestContentRejection(msg,_) =>
        complete(StatusCodes.BadRequest,msg)
      }
      .handle{ case MissingQueryParamRejection(msg) =>
        complete(StatusCodes.NotFound,s"missing required query parameter ${msg}")
      }
      .handle{ case AuthenticationFailedRejection(cause,challengeHeaders) =>
        val msg = cause match {
          case AuthenticationFailedRejection.CredentialsRejected =>
            "Invalid accessKey."
          case AuthenticationFailedRejection.CredentialsMissing =>
            "Missing accessKey."
        }
        complete(StatusCodes.Unauthorized,msg)
      }
      .handle{ case ChannelRejection(msg) =>
        complete(StatusCodes.Unauthorized,msg)
      }
      .handle{ case NonExistentAppRejection(msg) =>
        complete(StatusCodes.Unauthorized,msg)
      }.result()

  val exceptionHandler = ExceptionHandler{
    case e: StorageException =>{
      val msg = s"${e.getMessage()}"
      complete(StatusCodes.InternalServerError.value, msg)
    }
    case e: Exception => {
      val msg = s"${e.getMessage()}"
      complete(StatusCodes.InternalServerError.value, msg)
      //complete(HttpResponse(StatusCodes.InternalServerError,entity = msg))
    }
  }

  case class ChannelRejection(msg: String) extends Rejection
  case class NonExistentAppRejection(msg: String) extends Rejection
}