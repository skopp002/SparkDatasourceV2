package com.vmg.dsv2

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{
  InputPartition,
  InputPartitionReader
}
import org.apache.spark.sql.types.StructType

class MockInputPartitions(p: String,
                         m: String,
                         execDt: String,
                         url: String,
                         dataSchema: StructType)
  extends InputPartition[InternalRow] {
  // private val logger: Logger = Logger.getRootLogger
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    //  logger.info("Creating Partition Readers")
    new MockInputPartitionReader(p, m, execDt, url, dataSchema)
  }

}
