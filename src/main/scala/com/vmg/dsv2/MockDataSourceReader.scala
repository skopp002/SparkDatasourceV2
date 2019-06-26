package com.vmg.dsv2

import org.apache.spark.sql.types.DoubleType
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import java.util

class MockDataSourceReader(options: RestOptions) extends DataSourceReader {

    private val logger: Logger = Logger.getRootLogger

    override def readSchema(): StructType = StructType(
      StructField("metric", StringType, true) ::
        StructField("customer", StringType, true) ::
        StructField("pl", StringType, true) ::
        StructField("p", StringType, true) ::
        StructField("ma", StringType, true) ::
        StructField("ts", StringType, true) ::
        StructField("usg", DoubleType, true)
        :: Nil)
    // ScalaReflection.schemaFor[SchemaRepo.TargetSchema].dataType.asInstanceOf[StructType]

    lazy val inSchema: StructType = readSchema()

    override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
      logger.info("createInputPartitions")
      val inputPartitions = new java.util.ArrayList[InputPartition[InternalRow]]
      val ps: Array[String] = options.p.split("#")
      for (p <- ps) {
        val newp = p.stripSuffix("]").stripPrefix("[").split(",")
        if (newp.size == 2) {
          logger.info(s"Create a partition for ${newp(0)} and ${newp(1)}")
          inputPartitions.add(
            new MockInputPartitions(newp(0),
              newp(1),
              options.execDt,
              options.url,
              inSchema))
        } else {
          logger.info(s"Create a partition for ${newp(0)}")
          if (newp.size == 1) {
            inputPartitions.add(
              new MockInputPartitions(newp(0),
                "",
                options.execDt,
                options.url,
                inSchema))
          }
        }
      }
      inputPartitions
    }

}
