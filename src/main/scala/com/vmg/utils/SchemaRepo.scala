package com.vmg.utils

object SchemaRepo {
  case class MockTags(p: String,
                     s: String,
                     pl: String,
                     customer: String)
  case class MockData(metric: String, tags: MockTags, dps: Map[String, Double])

}
