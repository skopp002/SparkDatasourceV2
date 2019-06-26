package com.vmg.pipeline

import org.apache.spark.sql.SparkSession


object PullUsageData{

  def getSparkSession(appName: String = "mockDSV2", sparkMasterMode: String = "local", enableHiveSupport:Boolean = false)={
    val builder = SparkSession.builder
      .appName(appName)
      .master(sparkMasterMode)
      .config("spark.sql.session.timeZone", "UTC")
    if (enableHiveSupport) {
      /*
      As per the recommendations https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/cloud-integration.html
       */
      builder
        .config("hive.metastore.client.factory.class",
          "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true") //In spark shell spark.conf.set("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict") //spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.hadoop.hive.msck.path.validation", "ignore")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
          "2")
        .config(
          "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored",
          "true")
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.hive.metastorePartitionPruning", "true")
        .config("spark.sql.broadcastTimeout", "10800") //May be ok to eliminate this. Leaving it on until demo
    }
    builder.getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    //In production this is a bigger list of around 150 values and is pulled from another parquet file
    val fetchlst = List("abc")
    val url ="https://apiendpoint"
    val spark = getSparkSession()
    val usg = spark.read.format("com.vmg.dsv2")
                        .option("p_rows", fetchlst.mkString("#"))
                        .option("execdt", "20190622")
                        .option("url", url)
                        .load()

    usg.show(false)
  }
}
