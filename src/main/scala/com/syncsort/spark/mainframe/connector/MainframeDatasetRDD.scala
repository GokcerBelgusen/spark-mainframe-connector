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

import java.util.HashMap

import org.apache.spark.InterruptibleIterator
import org.apache.spark.Logging
import org.apache.spark.{Partition => SparkPartition}
import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.sqoop.common.MutableMapContext
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfiguration
import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration
import org.apache.sqoop.connector.mainframe.MainframeDatasetFTPExtractor
import org.apache.sqoop.connector.mainframe.MainframeDatasetFromInitializer
import org.apache.sqoop.connector.mainframe.MainframeDatasetPartition
import org.apache.sqoop.connector.mainframe.MainframeDatasetPartitioner
import org.apache.sqoop.job.etl.ExtractorContext
import org.apache.sqoop.job.etl.InitializerContext
import org.apache.sqoop.job.etl.{Partition => SqoopPartition}
import org.apache.sqoop.job.etl.PartitionerContext
import org.apache.sqoop.schema.{Schema => SqoopSchema}

class MainframeDatasetRDDPartition(
    rddId: Int,
    val index: Int,
    sqoopPartition: MainframeDatasetPartition) 
  extends SparkPartition {

  val serializableMainframeDatasetPartition = new SerializableWritable(sqoopPartition)

}
class MainframeDatasetRDD(
    sparkContext : SparkContext,
    parameters: Map[String, String])
  extends RDD[Row](sparkContext, Nil)
  with Logging {

  val username = parameters.getOrElse("username", null)
  val password = parameters.getOrElse("password", null)
  val uri = parameters.getOrElse("uri", null)
  val datasetName = parameters.getOrElse("datasetName", null)
  val numPartitions = parameters.getOrElse("numPartitions", "10").toInt
  var parameterHashMap : java.util.HashMap[String, String] = new java.util.HashMap[String, String]
  for (param <- sparkContext.getConf.getAll) {
    parameterHashMap.put(param._1, param._2)
  }

  private def initializeRDD = {
    val (linkConfiguration, fromJobConfiguration, mapContext) = getConfigs()
    val initializerContext = new InitializerContext(mapContext)
    new MainframeDatasetFromInitializer().initialize(initializerContext, linkConfiguration, fromJobConfiguration)
  }

  initializeRDD

  private def getConfigs() = {
    var linkConfiguration = new LinkConfiguration()
    linkConfiguration.linkConfig.username = username
    linkConfiguration.linkConfig.password = password
    linkConfiguration.linkConfig.uri = uri
    var fromJobConfiguration = new FromJobConfiguration()
    fromJobConfiguration.fromJobConfig.datasetName = datasetName
    var mapContext = new MutableMapContext(parameterHashMap)
    (linkConfiguration, fromJobConfiguration, mapContext)
  }

  override def getPartitions: Array[SparkPartition] = {
    val (linkConfiguration, fromJobConfiguration, mapContext) = getConfigs()
    val mainframeDatasetPartitioner = new MainframeDatasetPartitioner
    var partitionerContext = new PartitionerContext(mapContext, numPartitions, getSchema)
    var sqoopPartitionList = mainframeDatasetPartitioner.getPartitions(partitionerContext, linkConfiguration, fromJobConfiguration)
    val result = new Array[SparkPartition](sqoopPartitionList.size)
    for (i <- 0 until sqoopPartitionList.size) {
      result(i) = new MainframeDatasetRDDPartition(id, i, sqoopPartitionList.get(i).asInstanceOf[MainframeDatasetPartition])
    }
    result
  }

  override def compute(split: SparkPartition, taskContext: TaskContext) = {
    val (linkConfiguration, fromJobConfiguration, mapContext) = getConfigs()
    var extractorContext = new ExtractorContext(mapContext, null, getSchema)
    val mainframeDatasetRDDPartition = split.asInstanceOf[MainframeDatasetRDDPartition]
    val sqoopMainframeDatasetPartition = mainframeDatasetRDDPartition.serializableMainframeDatasetPartition.value.asInstanceOf[MainframeDatasetPartition]
    var mainframeDatasetExtractor = new MainframeDatasetFTPExtractor
    var sqoopIterator = mainframeDatasetExtractor.getIterator(extractorContext, linkConfiguration, fromJobConfiguration, sqoopMainframeDatasetPartition)
    val iter = new Iterator[Row] {
      override def hasNext: Boolean = {
         sqoopIterator.hasNext
      }
      override def next(): Row = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        new GenericRow(sqoopIterator.next.asInstanceOf[Array[Any]])
      }
    }
    new InterruptibleIterator(taskContext, iter)
  }

  // can be overridden by other classes
  def getSchema : SqoopSchema = {
    null
  }
}

