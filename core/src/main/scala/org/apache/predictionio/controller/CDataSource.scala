package org.apache.predictionio.controller

import grizzled.slf4j.Logger
import org.apache.predictionio.core.BaseDataSource
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Base class of a parallel data source with cache features.
  *
  *
  * @tparam TD Training data class
  * @tparam EI Evaluation information class
  * @tparam Q Query class
  * @tparam A Actual result class
  */
abstract class CDataSource [TD, EI, Q, A](params: Params)
  extends BaseDataSource[TD,EI,Q,A]{
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

  def readTrainingBase(sc: SparkContext): Seq[TD] = readTraining(sc)

  def readTraining(sc: SparkContext): Seq[TD] = {
    startTime.map{ st =>
      endTime.map{ et =>
        readTrainingRange(sc,st,et)
      }.getOrElse(
        //endTime not specify, read one day data
        readTrainingRange(sc,st,st.plusDays(1))
      )
    }.getOrElse {
      logger.error("DataSource startTime not specified. Abort.")
      sys.exit(1)
    }
  }

  def readTrainingRange(sc:SparkContext,st: DateTime,et: DateTime): Seq[TD] = {
    if(st.getMillis < et.getMillis){
      val daysCount = Days.daysBetween(st,et).getDays
      val ab = new ArrayBuffer[TD]
      (0 until daysCount).map(st.plusDays(_)).map{ dt =>
        ab.append(readOrLoadTrainingStream(sc,dt))
      }
      ab
    }else{
      logger.error("DataSource startTime larger than endTime. Time range error")
      sys.exit(1)
    }
  }

  def readOrLoadTrainingStream(sc: SparkContext,time: DateTime): TD = {
    val path = PathProvider.getPathByTime(baseURL,time)
    val res = loadCacheStream(sc,path)
    res match {
      case Success(data) =>
        data
      case Failure(e) =>
        logger.info(e.getMessage)
        logger.info("Cached DataSource not found, begin to read directly ...")
        val data = readTrainingStream(sc,time)
        writeCacheStream(sc,data,path)
        data
    }
  }

  def readTrainingStream(sc: SparkContext,time: DateTime): TD

  def loadCacheStream(sc: SparkContext,path: String): Try[TD]

  def writeCacheStream(sc: SparkContext, td: TD,path: String): Unit

  def readEvalBase(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] = readEval(sc)

  def readEval(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] =
    Seq[(TD, EI, RDD[(Q, A)])]()
}
