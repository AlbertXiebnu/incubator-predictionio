package org.apache.predictionio.controller

import grizzled.slf4j.Logger
import org.apache.predictionio.core.BaseDataSource
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
  @transient private var cachePath = "/"

  def readTrainingBase(sc: SparkContext): TD = readOrLoadTraining(sc)

  def readOrLoadTraining(sc: SparkContext): TD = {
    val res = loadCacheData(sc)
    res match {
      case Success(data) =>
        data
      case Failure(e) =>
        logger.info(e.getMessage)
        logger.info("Cached DataSource not found, begin to read directly ...")
        val data = readTraining(sc)
        writeCacheData(sc,data)
        data
    }
  }

  def readTraining(sc: SparkContext): TD

  def loadCacheData(sc: SparkContext): Try[TD]

  def writeCacheData(sc: SparkContext, td: TD): Unit

  def readEvalBase(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] = readEval(sc)

  def readEval(sc: SparkContext): Seq[(TD, EI, RDD[(Q, A)])] =
    Seq[(TD, EI, RDD[(Q, A)])]()
}
