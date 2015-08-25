/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.mainframe;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.job.etl.TransferableContext;

/**
 * Utilities for Mainframe.
 */
public class MainframeUtils {

  private static final Logger LOG = Logger.getLogger(MainframeUtils.class);
  public static void configurationToContext(Configuration configuration, MutableContext context) {
    for (Map.Entry<String, String> entry : configuration) {
      context.setString(entry.getKey(), entry.getValue());
    }
  }

  public static void contextToConfiguration(ImmutableContext context, Configuration configuration) {
    for (Map.Entry<String, String> entry : context) {
      configuration.set(entry.getKey(), entry.getValue());
    }
  }
  
  public static String getDatasetName(TransferableContext context, String datasetName) {
    String systemTypeString = context.getString("spark.mainframe.connector.system.type", "MVS");
    String fullDatasetName = null;
    if (systemTypeString.equals("MVS")) {
      fullDatasetName = "'" + datasetName + "'";
    }
    else if (systemTypeString.equals("UNIX")) {
      fullDatasetName = datasetName;
    }
    else {
      LOG.info("System type = " + systemTypeString);
      assert(false);
    }
    return fullDatasetName;
  }

}
