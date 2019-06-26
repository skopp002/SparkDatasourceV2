package com.vmg.utils

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object DateUtils {

  object DateFormats {
    val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")
    val YYYYMMDDHH = DateTimeFormat.forPattern("YYYYMMddHH")
    val YYYYMMDDHHMISS = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
    val YYYYWW = DateTimeFormat.forPattern("YYYYww")
    val YYYY_MM_DD = DateTimeFormat.forPattern("YYYY-MM-dd")
    val MONYYYY = DateTimeFormat.forPattern("MMM YYYY")
    val MONYY = DateTimeFormat.forPattern("MMM YY")
    val YYYYMM = DateTimeFormat.forPattern("YYYYMM")
    val SolrDT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val YYYYMMDDUTC = DateTimeFormat.forPattern("yyyyMMdd")
    //.withZoneUTC()
    val HHMMSS = DateTimeFormat.forPattern("HH:mm:ss")
    val MockDT = DateTimeFormat.forPattern("yyyy/MM/dd-HH:mm:ss")
    val prevDay: Long = 86400000
  }

  def getPrevdt(indt: String,
                infmt: DateTimeFormatter,
                outfmt: DateTimeFormatter): String = {
    try {
      infmt.parseDateTime(indt).minus(DateFormats.prevDay).toString(outfmt)
    } catch {
      case e: java.lang.NumberFormatException => indt
      case n: java.lang.NullPointerException  => ""
    }
  }


  def getdt(indt: String,
            infmt: DateTimeFormatter,
            outfmt: DateTimeFormatter): String = {
    try {
      infmt.parseDateTime(indt).toString(outfmt)
    } catch {
      case e: java.lang.NumberFormatException => indt
      case n: java.lang.NullPointerException  => ""
    }
  }

}
