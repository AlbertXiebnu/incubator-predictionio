package org.apache.predictionio.controller

import grizzled.slf4j.Logger
import org.apache.predictionio.core.BasePreparator
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Base class of a parallel preparator with cached
  *
  * @tparam TD Training data class
  * @tparam PD Prepared data class
  */
abstract class CPreparator[TD, PD](params: Params)
  extends BasePreparator[TD, PD]{
  @transient lazy val logger = Logger[this.type]
  @transient var startTime: Option[DateTime] = None
  @transient var endTime: Option[DateTime] = None
  @transient val formater = DateTimeFormat.forPattern("yyyyMMdd")

  if(params.beginDay == ""){
    startTime = None
  }else{
    startTime = Some(DateTime.parse(params.beginDay,formater))
  }

  if(params.endDay == ""){
    endTime = None
  }else{
    endTime = Some(DateTime.parse(params.endDay,formater))
  }

  def prepareBase(sc: SparkContext, td: Seq[TD]): PD = prepare(sc,td)

  def prepare(sc: SparkContext,td: Seq[TD]): PD = {
    startTime.map{ st =>
      endTime.map{ et =>
        val pds = prepareRange(sc,td,st,et)
        mergePrepareData(pds)
      }.getOrElse{
        val pds = prepareRange(sc,td,st,st.plusDays(1))
        mergePrepareData(pds)
      }
    }.getOrElse{
      logger.error("Preparator startTime not specified. Abort.")
      sys.exit(1)
    }
  }

  def prepareRange(sc: SparkContext,td: Seq[TD],st: DateTime,et: DateTime): Seq[PD] = {
    val daysCount = Days.daysBetween(st,et).getDays
    val daySeq = (0 until daysCount).map(st.plusDays(_))
    (daySeq zip td) map {
      case(day,data) =>
        prepareOrLoad(sc,data,day)
    }
  }

  def prepareOrLoad(sc: SparkContext, td: TD,time: DateTime): PD = {
    val path = PathProvider.getPathByTime(baseURL,time)
    val pdata = loadCachePrepare(sc,path)
    pdata match {
      case Success(data) =>
        data
      case Failure(e) =>
        logger.info(e.getMessage)
        logger.info("Cached PrepareData not found, begin to read directly ...")
        val data = prepareStream(sc,td)
        writeCachePrepare(sc,data,path)
        data
    }
  }

  def getPrepareData(sc: SparkContext,st: DateTime,et: DateTime): PD = {
    val daysCount = Days.daysBetween(st,et).getDays
    val buffer = new ArrayBuffer[PD]()
    (0 until daysCount).map(st.plusDays(_)).map{ day =>
      val path = PathProvider.getPathByTime(baseURL,day)
      val res = loadCachePrepare(sc,path)
      res match {
        case Success(data) =>
          buffer.append(data)
        case Failure(e) =>
          logger.info(s"get prepare data in ${path} failed")
      }
    }
    mergePrepareData(buffer)
  }

  def prepareStream(sc: SparkContext, trainingData: TD): PD

  def loadCachePrepare(sc: SparkContext, path: String): Try[PD]

  def writeCachePrepare(sc: SparkContext, pD: PD, path: String): Unit

  def mergePrepareData(pds: Seq[PD]): PD
}
