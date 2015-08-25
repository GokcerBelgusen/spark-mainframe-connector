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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

/**
 * Provides access to mainframe data from pure SQL statements.
 */
class DefaultSource
  extends RelationProvider {

  /**
   * Creates a new relation for mainframe data set given
   * username, password, uri, and dataset name as parameters.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    MainframeDatasetRelation(parameters)(sqlContext)
  }
}

object mainframe {

  /**
   * Adds a method, `mainframeDatasetFile`, to SQLContext that allows reading data stored as MainframeDataset.
   */
  implicit class MainframeDatasetContext(sqlContext: SQLContext) {
    def mainframeDataset(parameters: Map[String, String]) =
      sqlContext.baseRelationToDataFrame(MainframeDatasetRelation(parameters)(sqlContext))
  }

}

