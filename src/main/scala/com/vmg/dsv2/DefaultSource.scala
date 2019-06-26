package com.vmg.dsv2

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{
  DataSourceOptions,
  DataSourceV2,
  ReadSupport
}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val modOptions = new RestOptions(options)
    new MockDataSourceReader(modOptions)
  }
}

