package org.apache.predictionio.data.storage.hdfs

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.apache.predictionio.data.storage.Event
import org.json4s.native.JsonMethods
import org.json4s.{JArray, JField, JNothing, JObject, JString}

/**
  * Created by xie on 17/8/3.
  */
object HDFSUtils {

  val dateTimeFormatter = ISODateTimeFormat.dateTime().withOffsetParsed()
  val dateTimeNoMillisFormatter =
    ISODateTimeFormat.dateTimeNoMillis().withOffsetParsed()

  def generateId: String = java.util.UUID.randomUUID().toString.replace("-","")

  def stringToDateTime(dt: String): DateTime = {
    // We accept two formats.
    // 1. "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    // 2. "yyyy-MM-dd'T'HH:mm:ssZZ"
    // The first one also takes milliseconds into account.
    try {
      // formatting for "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
      dateTimeFormatter.parseDateTime(dt)
    } catch {
      case e: IllegalArgumentException => {
        // handle when the datetime string doesn't specify milliseconds.
        dateTimeNoMillisFormatter.parseDateTime(dt)
      }
    }
  }

  def dateTimeToString(dt: DateTime): String = dateTimeFormatter.print(dt)

  def eventToJsonString(eventId: String,appId: Int,channelId: Option[Int], event: Event): String = {
    val obj = JObject(
                JField("eventId",JString(eventId)) ::
                JField("event", JString(event.event)) ::
                JField("entityType", JString(event.entityType)) ::
                JField("entityId", JString(event.entityId)) ::
                JField("targetEntityType",
                  event.targetEntityType.map(JString(_)).getOrElse(JString(""))) ::
                JField("targetEntityId",
                  event.targetEntityId.map(JString(_)).getOrElse(JString(""))) ::
                JField("properties", event.properties.toJObject) ::
                JField("eventTime", JString(dateTimeToString(event.eventTime))) ::
                JField("tags", JArray(event.tags.toList.map(JString(_)))) ::
                JField("prId",
                  event.prId.map(JString(_)).getOrElse(JString(""))) ::
                JField("creationTime",
                  JString(dateTimeToString(event.creationTime))) ::
                Nil)
    val body_str = JsonMethods.compact(JsonMethods.render(obj))
    val flumeJson = JArray(List(
      JObject(
        JField("headers",JObject(
          JField("appId",JString(appId.toString)) ::
          JField("channelId",JString(channelId.getOrElse(0).toString())) :: Nil)) ::
        JField("body",JString(body_str)) ::
        Nil
      )
    ))
    JsonMethods.compact(JsonMethods.render(flumeJson))
  }
}
