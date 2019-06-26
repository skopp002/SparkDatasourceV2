package com.vmg.dsv2

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import com.vmg.utils.DateUtils._
import com.vmg.utils.SchemaRepo

class MockInputPartitionReader(p: String,
                              m: String,
                              execDt: String,
                              url: String,
                              dataSchema: StructType)
  extends InputPartitionReader[InternalRow] {

  val RESTCONNECTIONTIMEOUT = 500
  val RESTREADTIMEOUT = 1000
  private val logger: Logger = Logger.getRootLogger

  val dpsrows = ListBuffer[InternalRow]()

  val pusage = processMockMetrics

  private def getMockAPI(p: String,
                        connectTimeout: Int,
                        readTimeout: Int,
                        requestMethod: String = "GET"): Try[String] = {
    import java.net.{URL, HttpURLConnection}
    val mStartDt =
      getPrevdt(execDt,
        DateFormats.YYYYMMDD,
        DateFormats.MockDT)
    val mEndDt = getdt(execDt,
      DateFormats.YYYYMMDD,
      DateFormats.MockDT)

    val modurl =
      s"""${url}start=$mStartDt&end=$mEndDt&m=sum:5m-avg-none:usg{customer=*,pl=*,pl=${p.toLowerCase}}"""
    logger.info(s"Fetching $modurl")
    val connection =
      (new URL(modurl)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    /*
    This is what is used in prod
    val res = Try(scala.io.Source.fromURL(url).getLines().mkString)
    */
    val res = Try(scala.io.Source.fromFile("src/main/resources/mockdata.json").getLines().mkString)
    logger.info(s"Fetched data for $p")
    res
  }

  private def processMockMetrics = {
    getMockAPI(p, RESTCONNECTIONTIMEOUT, RESTREADTIMEOUT) match {
      case Success(data) =>
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats = DefaultFormats
        logger.info(s"Parsing json successful response")
        val js = parse(data).extract[List[SchemaRepo.MockData]]
        logger.info(s"Parsed json successful response")
        for (i <- 0 to (js.length - 1)) {
          logger.info(s"Iterating over dps for $p with ${js.length} elements")
          val dpsCnt = js(i).dps.size
          val metricUnit = js(i).dps.map { dp =>
            logger.info(s"Creating dps row for $p with $dpsCnt values")
            dpsrows.append(
              InternalRow(
                UTF8String.fromString(js(i).metric),
                UTF8String.fromString(js(i).tags.customer),
                UTF8String.fromString(js(i).tags.pl),
                UTF8String.fromString(js(i).tags.p),
                UTF8String.fromString(m),
                UTF8String.fromString(dp._1),
                dp._2
              ))
          }
        }
      case Failure(f) =>
        logger.info(s"Failed to receive successful response for $p")
        dpsrows.append(
          InternalRow(
            UTF8String.fromString("usg"),
            UTF8String.fromString(f.getMessage),
            UTF8String.fromString(""),
            UTF8String.fromString(p),
            UTF8String.fromString(m),
            UTF8String.fromString(""),
            0.0
          ))

    }
    dpsrows.toList
  }

  override def next: Boolean = {
    logger.info(s"Next $p")
    pusage.iterator.hasNext
  }

  override def get: InternalRow = {
    logger.info(s"Get $p")
    pusage.iterator.next()
  }

  override def close(): Unit = Unit

}
