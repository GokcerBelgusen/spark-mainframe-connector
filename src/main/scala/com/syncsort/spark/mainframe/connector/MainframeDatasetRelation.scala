/*
 * Copyright 2015 Syncsort
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.syncsort.spark.mainframe.connector

import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}


case class MainframeDatasetRelation(
    parameters: Map[String, String])
      (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging {
  var mainframeDatasetSchema : StructType = StructType(StructField("record", StringType, false) :: Nil)

  override val schema: StructType = getMainframeDatasetSchema

  val connectorClass = parameters.getOrElse("connectorClass", "mainframe")
  if (connectorClass != "mainframe") {
    throw new RuntimeException("Connector class " + connectorClass + " is not supported")
  }
  override def buildScan(): RDD[Row] = {
    val baseRdd = new MainframeDatasetRDD(sqlContext.sparkContext, parameters)
    baseRdd
  }

  protected def getMainframeDatasetSchema = mainframeDatasetSchema
}
