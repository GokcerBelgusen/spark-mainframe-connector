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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.MainframeConnectorError;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

public class MainframeDatasetPartitioner extends Partitioner<LinkConfiguration, FromJobConfiguration> {

  private static final Logger LOG =
    Logger.getLogger(MainframeDatasetPartitioner.class);

  @Override
  public List<Partition> getPartitions(PartitionerContext context,
                                       LinkConfiguration linkConfiguration,
                                       FromJobConfiguration fromJobConfig) {

    List<Partition> partitions = new ArrayList<Partition>();
    String dsName = fromJobConfig.fromJobConfig.datasetName;
    LOG.info("Datasets to transfer from: " + dsName);
    try {
      List<String> datasets = retrieveDatasets(dsName, context, linkConfiguration);
      if (datasets.isEmpty()) {
        throw new SqoopException(MainframeConnectorError.GENERIC_MAINFRAME_CONNECTOR_0000, "No sequential datasets retrieved from " + dsName);
      } else {
        int count = datasets.size();
        int chunks = (int) Math.min(count, context.getMaxPartitions());
        for (int i = 0; i < chunks; i++) {
          partitions.add(new MainframeDatasetPartition());
        }

        int j = 0;
        while(j < count) {
          for (Partition partition : partitions) {
            if (j == count) {
              break;
            }
            ((MainframeDatasetPartition)partition).addDataset(datasets.get(j));
            j++;
          }
        }
      }
      return partitions;
    }
    catch (IOException ioe) {
      throw new SqoopException(MainframeConnectorError.GENERIC_MAINFRAME_CONNECTOR_0000, ioe.toString());
    }
  }

  protected List<String> retrieveDatasets(String dsName, PartitionerContext context, LinkConfiguration linkConfiguration)
      throws IOException {
    return MainframeFTPClientUtils.listSequentialDatasets(dsName, context, linkConfiguration);
  }
}

