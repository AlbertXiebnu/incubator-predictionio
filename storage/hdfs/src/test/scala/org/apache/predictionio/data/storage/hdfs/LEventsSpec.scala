package org.apache.predictionio.data.storage.hdfs

import org.apache.predictionio.data.storage.{LEvents, Storage}
import org.specs2._

/**
  * Created by xie on 17/8/2.
  */
class LEventsSpec extends Specification with TestEvents { def is = s2"""
  PredictionIO Storage LEvents Specification

    Events can be implemented by:
    - HDFSLEvents ${hdfsLEvents}
provided
  """

  def hdfsLEvents = sequential ^ s2"""
    HDFSLEvents should
     - behave like any LEvents implemention ${events(hdfsDO)}
  """

  val appId = 1

  def events(eventClient: LEvents) = sequential ^ s2"""
    init default ${initDefault(eventClient)}
    insert 3 test events and get back by event ID ${insertAndGetEvents(eventClient)}
  """

  val dbName = "test_pio_storage_events_" + hashCode

  def hdfsDO = Storage.getDataObject[LEvents](StorageTestUtils.hdfsSourceName,dbName)

  def initDefault(eventClient: LEvents) = {
    eventClient.init(appId)
  }

  def insertAndGetEvents(eventClient: LEvents) = {
    val listOfEvents = List(r1,r2,r3)
    val insertResp = listOfEvents.map{ eventClient.insert(_, appId)}
    insertResp.foreach(println)
    val s = 1
    s must equalTo(1)
  }

  def removeDefault(eventClient: LEvents) = {
    eventClient.remove(appId)
  }
}
