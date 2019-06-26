package com.vmg.dsv2

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
 *
 */

import java.io._
import java.util
import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions

// scalastyle:off
@SerialVersionUID(123L)
class RestOptions(options: DataSourceOptions) extends Serializable {

  import RestOptions._

  protected var keyLowerCasedMap: util.Map[String, String] =
    new java.util.HashMap[String, String](options.asMap())
  val url: String = {
    var tempStr = "https://apiendpoint.This will be ignored anyways"//keyLowerCasedMap.getOrDefault(url.toLowerCase, "")
    if (tempStr == "") {
      //throw new RESTOptionsException("Required option url.")
      println("Url is empty")
    }
    tempStr
  }

  val p: String = {
    var tempStr = keyLowerCasedMap.getOrDefault(p_rows, "")
    if (tempStr == "") {
      throw new RESTOptionsException("Missing p")
    }
    tempStr
  }


  val execDt: String = {
    var tempStr = keyLowerCasedMap.getOrDefault(execdt, "")
    if (tempStr == "") {
      throw new RESTOptionsException("Missing execdt")
    }
    tempStr
  }

  protected def toLowerCase(key: String) = key.toLowerCase(Locale.ROOT)

  def empty = new DataSourceOptions(new util.HashMap[String, String])
  def asMap = new util.HashMap[AnyRef, AnyRef](keyLowerCasedMap)

  /**
    * Returns the option value to which the specified key is mapped, case-insensitively.
    */
  def get(key: String): Option[String] =
    Option(keyLowerCasedMap.get(toLowerCase(key)))

  /**
    * Returns the boolean value to which the specified key is mapped,
    * or defaultValue if there is no mapping for the key. The key match is case-insensitive
    */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    val lcaseKey = toLowerCase(key)
    if (keyLowerCasedMap.containsKey(lcaseKey)) {
      keyLowerCasedMap.get(lcaseKey).toBoolean
    } else {
      defaultValue
    }
  }

  /**
    * Returns the integer value to which the specified key is mapped,
    * or defaultValue if there is no mapping for the key. The key match is case-insensitive
    */
  def getInt(key: String, defaultValue: Int): Int = {
    val lcaseKey = toLowerCase(key)
    if (keyLowerCasedMap.containsKey(lcaseKey)) {
      keyLowerCasedMap.get(lcaseKey).toInt
    } else {
      defaultValue
    }
  }

  /**
    * Returns the long value to which the specified key is mapped,
    * or defaultValue if there is no mapping for the key. The key match is case-insensitive
    */
  def getLong(key: String, defaultValue: Long): Long = {
    val lcaseKey = toLowerCase(key)
    if (keyLowerCasedMap.containsKey(lcaseKey))
      keyLowerCasedMap.get(lcaseKey).toLong
    else defaultValue
  }

  /**
    * Returns the double value to which the specified key is mapped,
    * or defaultValue if there is no mapping for the key. The key match is case-insensitive
    */
  def getDouble(key: String, defaultValue: Double): Double = {
    val lcaseKey = toLowerCase(key)
    if (keyLowerCasedMap.containsKey(lcaseKey))
      keyLowerCasedMap.get(lcaseKey).toDouble
    else defaultValue
  }
}

/* An object is a type of class that can have no more than one instance, known in object-oriented design as a singleton.
   An object with the same name as a class is called a companion object. Conversely, the class is the object’s companion class.
   A companion class or object can access the private members of its companion.
   Use a companion object for methods and values which are not specific to instances of the companion class.
 */
object RestOptions extends RESTOptionObjectMethods {
  val url: String = newOption("url")
  val p_rows = newOption("p_rows")
  val execdt = newOption("execdt")
}

trait RESTOptionObjectMethods {
  protected val restOptionNames: collection.mutable.Set[String] =
    collection.mutable.Set[String]()
  protected def newOption(name: String): String = {
    restOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }
}

class RESTOptionsException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}

class RESTOptionsExceptionRuntimeException(message: String)
  extends RuntimeException(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}

