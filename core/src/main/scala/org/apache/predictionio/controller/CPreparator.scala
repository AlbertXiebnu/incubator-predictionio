package org.apache.predictionio.controller

import grizzled.slf4j.Logger
import org.apache.predictionio.core.BasePreparator
import org.apache.spark.SparkContext

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

  def prepareBase(sc: SparkContext, td: TD): PD = {
    prepareOrLoad(sc, td)
  }

  def prepareOrLoad(sc: SparkContext, td: TD): PD = {
    val pdata = loadCachePrepare(sc)
    pdata match {
      case Success(data) =>
        data
      case Failure(e) =>
        logger.info(e.getMessage)
        logger.info("Cached PrepareData not found, begin to read directly ...")
        val data = prepare(sc,td)
        writeCachePrepare(sc,data)
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

  def loadCachePrepare(sc: SparkContext): Try[PD]

  def writeCachePrepare(sc: SparkContext, pD: PD): Unit

}
