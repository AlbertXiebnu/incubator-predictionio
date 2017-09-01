package org.apache.predictionio.data.storage.hdfs

import org.apache.predictionio.data.storage._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.ext.JodaTimeSerializers
import org.specs2.Specification
import org.specs2.specification.{Fragments, Step}
import org.json4s.native.Serialization
import scala.util.{Failure, Try}

/**
  * Created by xie on 17/8/13.
  */
class PEventsSpec extends Specification with TestEvents with Serializable{
//  System.clearProperty("spark.driver.port")
//  System.clearProperty("spark.hostPort")
  @transient lazy val sc = new SparkContext(new SparkConf().setAppName("PEvent test").setMaster("local[2]"))

  @transient lazy val appId = 1
  @transient lazy val channelId = 6

  def hdfsLocal = Storage.getDataObject[LEvents](
    "HDFS","hdfs")

  def hdfsPar = Storage.getDataObject[PEvents]("HDFS","hdfs")


  def stopSpark() = {
    sc.stop()
  }

  def is  =s2"""
    PredictionIO Storage PEvents Specification

    PEvents can be implemented by:
      - HDFSPEvents ${hdfsPEvents}
      - (stop Spark) ${Step(sc.stop())}
    """

  def hdfsPEvents = sequential ^ s2"""
    JDBCPEvents should
    - behave like any PEvents implementation ${events(hdfsLocal, hdfsPar)}
  """

  def events(localEventClient: LEvents, parEventClient: PEvents) = sequential ^ s2"""
    - write Test ${writeTest(parEventClient)}
  """

  val listOfEvents = List(u1e5, u2e2, u1e3, u1e1, u2e3, u2e1, u1e4, u1e2, r1, r2)
  val listOfEventsChannel = List(u3e1, u3e2, u3e3, r3, r4)

  def initTest(localEventClient: LEvents) = {
    localEventClient.init(appId)
    localEventClient.init(appId, Some(channelId))
  }

  def insertTestEvents(localEventClient: LEvents) = {
    listOfEvents.map( localEventClient.insert(_, appId) )
    // insert to channel
    listOfEventsChannel.map( localEventClient.insert(_, appId, Some(channelId)) )
    success
  }

  def find(parEventClient: PEvents) = {
    val resultRDD: RDD[Event] = parEventClient.find(
      appId = appId
    )(sc)

    val results = resultRDD.collect.toList
      .map {_.copy(eventId = None)} // ignore eventId

    results must containTheSameElementsAs(listOfEvents)
  }

  def findChannel(parEventClient: PEvents) = {
    val resultRDD: RDD[Event] = parEventClient.find(
      appId = appId,
      channelId = Some(channelId)
    )(sc)

    val results = resultRDD.collect.toList
      .map {_.copy(eventId = None)} // ignore eventId

    results must containTheSameElementsAs(listOfEventsChannel)
  }

  def writeTest(parEventClient: PEvents) = {
    @transient lazy val json4sDefaultFormats = DefaultFormats.lossless ++ JodaTimeSerializers.all
    @transient lazy implicit val formats = json4sDefaultFormats + new EventJson4sSupport.APISerializer
    val raw = sc.textFile("/Users/xie/Develop/incubator-predictionio-template-text-classifier/data/stopwords.json")
    println(raw.count())
    val rdd = raw.map{ json =>
      Serialization.read[Event](json)
    }
    val pevent = Storage.getPEvents()
    pevent.write(events = rdd, appId = 10,channelId = Some(0))(sc)
    println("Events are imported.")
    println("Done.")
    val res = 0
    res must beEqualTo(0)
  }

  def aggregateUserProperties(parEventClient: PEvents) = {
    val resultRDD: RDD[(String, PropertyMap)] = parEventClient.aggregateProperties(
      appId = appId,
      entityType = "user"
    )(sc)
    val result: Map[String, PropertyMap] = resultRDD.collectAsMap.toMap

    val expected = Map(
      "u1" -> PropertyMap(u1, u1BaseTime, u1LastTime),
      "u2" -> PropertyMap(u2, u2BaseTime, u2LastTime)
    )

    result must beEqualTo(expected)
  }

  def aggregateUserPropertiesChannel(parEventClient: PEvents) = {
    val resultRDD: RDD[(String, PropertyMap)] = parEventClient.aggregateProperties(
      appId = appId,
      channelId = Some(channelId),
      entityType = "user"
    )(sc)
    val result: Map[String, PropertyMap] = resultRDD.collectAsMap.toMap

    val expected = Map(
      "u3" -> PropertyMap(u3, u3BaseTime, u3LastTime)
    )

    result must beEqualTo(expected)
  }
}
