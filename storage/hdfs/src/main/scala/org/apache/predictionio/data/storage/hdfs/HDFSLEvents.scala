/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.predictionio.data.storage.hdfs

import grizzled.slf4j.Logging
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.predictionio.data.storage.{Event, LEvents, StorageClientConfig}
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

/**
  * HDFS implement of [[LEvents]]
  * Created by xie on 17/8/1.
  */
class HDFSLEvents(
    clientMap: Map[String,AnyRef],
    config: StorageClientConfig,
    prefix: String) extends LEvents with Logging{

  implicit private val formats = org.json4s.DefaultFormats

  private val flumeHost = clientMap("FlumeClient").toString

  def init(appId: Int, channelId: Option[Int]): Boolean = {
    debug("init call")
    true
  }

  def remove(appId: Int, channelId: Option[Int]): Boolean = {
    info("remove not support in HDFS Event Store")
    false
  }

  def close(): Unit = {
    //nothing to do
  }

  def futureInsert(event: Event, appId: Int, channelId: Option[Int])(
    implicit ec: ExecutionContext): Future[String] = Future {
    val id = event.eventId.getOrElse(HDFSUtils.generateId)
    var jsonstr = ""
    try {
      jsonstr = HDFSUtils.eventToJsonString(id, appId, channelId, event)
      val post_req = new HttpPost(flumeHost)
      post_req.setHeader("Content-type","application/json")
      post_req.setEntity(new StringEntity(jsonstr))
      val response = (new DefaultHttpClient()).execute(post_req)
      id
    }catch {
      case e: Throwable =>
        e.getMessage()
    }
  }

  def futureFind(
     appId: Int,
     channelId: Option[Int],
     startTime: Option[DateTime],
     untilTime: Option[DateTime],
     entityType: Option[String],
     entityId: Option[String],
     eventNames: Option[Seq[String]],
     targetEntityType: Option[Option[String]],
     targetEntityId: Option[Option[String]],
     limit: Option[Int],
     reversed: Option[Boolean]
     )(implicit ec: ExecutionContext): Future[Iterator[Event]] = Future{
    logger.info("futureFind not support in HDFS Event Store")
    Iterator()
  }

  def futureGet(eventId: String, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext): Future[Option[Event]] = Future{
    logger.info("futureGet not support in HDFS Event Store")
    None
  }

  def futureDelete(eventId: String, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext): Future[Boolean] = Future{
    logger.info("futureDelete not support in HDFS Event Store")
    false
  }
}
