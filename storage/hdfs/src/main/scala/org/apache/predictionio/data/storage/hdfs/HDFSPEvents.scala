package org.apache.predictionio.data.storage.hdfs

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.predictionio.data.storage.hdfs.HDFSUtils.dateTimeToString
import org.apache.predictionio.data.storage.{DataMap, Event, PEvents, StorageClientConfig}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{from_utc_timestamp, unix_timestamp}
import org.json4s.{JArray, JField, JObject, JString}
import org.json4s.native.{JsonMethods, Serialization}

/**
  * Created by xie on 17/8/10.
  */
class HDFSPEvents(@transient clientMap: Map[String, AnyRef], config: StorageClientConfig, prefix: String) extends PEvents {

  @transient private implicit lazy val formats = org.json4s.DefaultFormats
  @transient private final lazy val rootPath = clientMap("RootPath").asInstanceOf[String]
  @transient private final val fs = clientMap("HDFSClient").asInstanceOf[org.apache.hadoop.fs.FileSystem]
  @transient private final val dataFormat = clientMap("Format").asInstanceOf[String]
  @transient private final val delimiter = clientMap("Delimiter").asInstanceOf[String]
  @transient private final val beginDateDefault = "19720101"
  @transient private final val endDateDefault = "21000101"
  @transient private final val beginHourDefault = "00"
  @transient private final val endHourDefault = "24"

  private final val eventSchema = StructType(
    StructField("eventId",StringType) ::
      StructField("event",StringType) ::
      StructField("entityType",StringType) ::
      StructField("entityId",StringType) ::
      StructField("targetEntityType",StringType) ::
      StructField("targetEntityId",StringType) ::
      StructField("properties",StringType) ::
      StructField("eventTime",StringType) ::
      StructField("tags",StringType) ::
      StructField("prId",StringType) ::
      StructField("creationTime",StringType) ::
      Nil
  )

  override def find(
    appId: Int,
    channelId: Option[Int] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None)(sc: SparkContext): RDD[Event] = {
    val sqlContext = new SQLContext(sc)
    val basePath = rootPath + s"/app_$appId/channel_${channelId.getOrElse(0)}"

    val lower = startTime.map(_.getMillis).getOrElse(0.toLong)
    val upper = untilTime.map(_.getMillis).getOrElse((DateTime.now + 99.years).getMillis)
    val beginDate = startTime.map(t => t.toString("yyyyMMdd")).getOrElse(beginDateDefault)
    val endDate = untilTime.map(t => t.toString("yyyyMMdd")).getOrElse(endDateDefault)
    val beginHour = startTime.map(t => t.toString("HH")).getOrElse(beginHourDefault)
    val endHour = untilTime.map(t => t.toString("HH")).getOrElse(endHourDefault)
    val fileList = getInputPathList(basePath,beginDate,endDate,beginHour,endHour)
    val inputFiles = fileList.map(f => f + "/*")
//    logger.info(inputFiles.mkString(","))

    val entityTypeClause = entityType.map(e => s" and entityType='$e'").getOrElse("")
    val entityIdClause = entityId.map(e => s" and entityId='$e'").getOrElse("")
    val eventNamesClause =
      eventNames.map("and (" + _.map(y => s"event = '$y'").mkString(" or ") + ")").getOrElse("")
    val targetEntityTypeClause = targetEntityType.map(
      _.map(e =>
        s"and targetEntityType = '$e'").
        getOrElse("and targetEntityType is null")
    ).getOrElse("")
    val targetEntityIdClause = targetEntityId.map(
      _.map(e =>
        s"and targetEntityId = '$e'"
      ).getOrElse("and targetEntityId is null")
    ).getOrElse("")

    val tableName = "eventTable"
    val statement =
      s"""
         |select
         |eventId,
         |event,
         |entityType,
         |entityId,
         |targetEntityType,
         |targetEntityId,
         |properties,
         |eventTime,
         |tags,
         |prId,
         |creationTime,
         |timestamp
         |from ${tableName}
         |where
         |timestamp >= ${lower/1000} and
         |timestamp < ${upper/1000}
         |${entityTypeClause}
         |${entityIdClause}
         |${eventNamesClause}
         |${targetEntityTypeClause}
         |${targetEntityIdClause}
         """.stripMargin.replace("\n"," ")
    logger.info(statement)

    val df = readAsDataFrame(sqlContext,inputFiles)
    // convert eventTime as unix timestamp from 1972-01-01 in second
    val dfTimestamp = df.withColumn("timestamp",unix_timestamp(
      from_utc_timestamp(df("eventTime"),"yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
    ))
    dfTimestamp.registerTempTable(tableName)
//    dfTimestamp.show()

    val sqlDF = sqlContext.sql(statement)
//    sqlDF.show()

    sqlDF.rdd.map(row =>
      Event(
        eventId = Option(row.getString(0)),
        event = row.getString(1),
        entityType = row.getString(2),
        entityId = row.getString(3),
        targetEntityType = Option(row.getString(4)),
        targetEntityId = Option(row.getString(5)),
        properties = Option(row.getString(6)).map( x =>
          DataMap(Serialization.read[JObject](x))).getOrElse(DataMap()),
        eventTime = DateTime.parse(row.getString(7)),
        tags = Option(row.getString(8)).map(x =>
          x.replace("[","").replace("]","").split(",").toList).getOrElse(Nil),
        prId = Option(row.getString(9)),
        creationTime = DateTime.parse(row.getString(10))
      )).cache()
  }

  override def write(events: RDD[Event], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit = {
//    val eventRDD = events.map( x =>
//      Row(HDFSUtils.generateId,x.event,x.entityType,x.entityId,x.targetEntityType,x.targetEntityId,
//          x.eventTime,x.tags,x.prId,x.creationTime)
//    )
//    val sqlContext = new SQLContext(sc)
//    val eventDF = sqlContext.createDataFrame(eventRDD,eventSchema)
    val path = rootPath + s"/app_$appId/channel_${channelId.getOrElse(0)}/${DateTime.now.toString("yyyyMMdd")}/${DateTime.now.toString("HH")}"
    val name = "ImportData." + DateTime.now.getMillis
    val savePath = path + "/" + name
    dataFormat match {
      case "json" =>
        events.map{ event =>
          val obj = JObject(
            JField("eventId",JString(HDFSUtils.generateId)) ::
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
              Nil
          )
          obj.toString
        }.saveAsTextFile(savePath)
      case "csv" =>
        val rdd = events.map{ e =>
          val eventId = HDFSUtils.generateId
          val event = e.event
          val entityType = e.entityType
          val entityId = e.entityId
          val targetEntityType = e.targetEntityId.getOrElse("")
          val targetEntityId = e.targetEntityId.getOrElse("")
          val properties = JsonMethods.compact(JsonMethods.render(e.properties.toJObject))
          val eventTime = dateTimeToString(e.eventTime)
          val tags = e.tags.toList.mkString(",")
          val prId = e.prId.getOrElse("")
          val creationTime = dateTimeToString(e.creationTime)
          Seq(eventId,
              event,
              entityType,
              entityId,
              targetEntityType,
              targetEntityId,
              properties,
              eventTime,
              tags,
              prId,
              creationTime
          ).mkString("\t")
        }
        rdd.saveAsTextFile(savePath)
      case _ =>
        logger.error("data format not supported. current only support csv and json")
    }



  }

  override def delete(eventIds: RDD[String], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit = {
    //no implement
    logger.info("HDFSPEvents.delete(...) not support in HDFS Event Store")
  }

  private def readAsDataFrame(sqlContext: SQLContext,paths: Array[String]): DataFrame = {
    dataFormat match {
      case "json" =>
        sqlContext.read.schema(eventSchema).json(paths: _*)
      case "csv" =>
        paths.map(path =>
          sqlContext.read.format("csv").
            option("header","false").
            option("delimiter",delimiter).
            schema(eventSchema).load(path)
        ).reduce((d1,d2) => d1.unionAll(d2))
    }
  }

  private def getInputPathList(basePath: String,beginDate: String,endDate: String,
                              beginHour: String, endHour: String) = {
    val status = fs.listStatus(new Path(basePath))
    status.filter(s => {
      val p = s.getPath()
      (s.isDirectory && p.getName >= beginDate && p.getName <=endDate)
    }).map(s => fs.listStatus(s.getPath)).flatten
    .filter(s => {
      val p = s.getPath()
      (s.isDirectory && p.getName >= beginHour && p.getName <= endHour)
    }).map(s => s.getPath.toUri.getPath)
  }
}
