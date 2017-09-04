package org.apache.predictionio.controller

import org.joda.time.DateTime

/**
  * Created by xie on 17/9/1.
  */
object PathProvider {
  /**
    * Get Save hdfs path according to currentTime
    * @return
    */
  def getCurrentPath(baseURL: String): String = {
    val date = DateTime.now().toString("yyyyMMdd")
    baseURL.trim().stripSuffix("/")+"/" + date
  }

  def getPathByTime(baseURL: String,time: DateTime): String = {
    val date = time.toString("yyyyMMdd")
    baseURL.trim().stripSuffix("/")+"/" + date
  }
}
