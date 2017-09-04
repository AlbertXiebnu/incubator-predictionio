package org.apache.predictionio.controller

import grizzled.slf4j.Logger
import org.apache.predictionio.core.BasePreparator
import org.apache.spark.SparkContext
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

/**
  * Base class of a parallel preparator with cached
  *
  * @tparam TD Training data class
  * @tparam PD Prepared data class
  */
abstract class CPreparator[TD, PD]
  extends BasePreparator[TD, PD]{
  @transient lazy val logger = Logger[this.type]
  @transient var startTime: Option[DateTime] = None
  @transient var endTime: Option[DateTime] = None

  def prepareBase(sc: SparkContext, td: TD): PD = {
    prepareOrLoad(sc, td)
  }

  def prepareOrLoad(sc: SparkContext, td: TD): PD = {
    val path = PathProvider.getPathByTime(baseURL,startTime.getOrElse(DateTime.now()))
    val pdata = loadCachePrepare(sc,path)
    pdata match {
      case Success(data) =>
        data
      case Failure(e) =>
        logger.info(e.getMessage)
        logger.info("Cached PrepareData not found, begin to read directly ...")
        val data = prepare(sc,td)
        writeCachePrepare(sc,data,path)
        data
    }
  }
  /** Implement this method to produce prepared data that is ready for model
    * training.
    *
    * @param sc An Apache Spark context.
    * @param trainingData Training data to be prepared.
    */
  def prepare(sc: SparkContext, trainingData: TD): PD

  def loadCachePrepare(sc: SparkContext, path: String): Try[PD]

  def writeCachePrepare(sc: SparkContext, pD: PD, path: String): Unit


}
