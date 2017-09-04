package org.apache.predictionio.controller

import grizzled.slf4j.Logger
import org.apache.predictionio.core.BaseDataSource
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

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
abstract class CDataSource [TD, EI, Q, A]
  extends BaseDataSource[TD,EI,Q,A]{
  @transient lazy val logger = Logger[this.type]
  @transient var startTime: Option[DateTime] = None
  @transient var endTime: Option[DateTime] = None

  def readTrainingBase(sc: SparkContext): TD = readOrLoadTraining(sc)

  def readTrainingByTimeRange(sc: SparkContext): RDD[Event] = {
    startTime.map{ st =>
      endTime.map{ et =>
        PEventStore.find(
          appName = "ctr",
          startTime = Some(st),
          untilTime = Some(et),
          entityType = Some("ad"))(sc)
      }.getOrElse{
        val end = st.plusDays(1)
        PEventStore.find(appName = "ctr",
          startTime = Some(st),
          untilTime = Some(end),
          entityType = Some("ad"))(sc)
      }
    }.getOrElse{
      PEventStore.find(appName = "ctr",entityType = Some("ad"))(sc)
    }
  }

  def readTrainingRaw(sc: SparkContext): TD = {
    val eventRDD = readTrainingByTimeRange(sc)
    readTraining(sc,eventRDD)
  }

  def readOrLoadTraining(sc: SparkContext): TD = {
    val path = PathProvider.getPathByTime(baseURL,startTime.getOrElse(DateTime.now()))
    val res = loadCacheData(sc,path)
    res match {
      case Success(data) =>
        data
      case Failure(e) =>
        logger.info(e.getMessage)
        logger.info("Cached DataSource not found, begin to read directly ...")
        val data = readTrainingRaw(sc)
        writeCacheData(sc,data,path)
        data
    }
  }

  def readTraining(sc: SparkContext,rdd: RDD[Event]): TD

  def loadCacheData(sc: SparkContext,path: String): Try[TD]

  def writeCacheData(sc: SparkContext, td: TD,path: String): Unit

  def readEvalBase(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] = readEval(sc)

  def readEval(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] =
    Seq[(TD, EI, RDD[(Q, A)])]()
}
