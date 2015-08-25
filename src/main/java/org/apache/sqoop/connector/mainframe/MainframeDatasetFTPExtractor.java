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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfig;
import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.MainframeConnectorError;
import org.apache.sqoop.job.etl.ExtractorContext;

/**
 * Extract from Mainframe.
 */
public class MainframeDatasetFTPExtractor extends MainframeDatasetExtractor {

  public static final Logger LOG = Logger.getLogger(MainframeDatasetFTPExtractor.class);

  private FTPClient ftp = null;
  private BufferedReader datasetReader = null;
  private String parentDatasetName = null;

  @Override
  protected void initializeConnection(ExtractorContext context, LinkConfiguration linkConfiguration, 
                          FromJobConfiguration jobConfiguration, MainframeDatasetPartition inputPartition) 
               throws IOException {
    parentDatasetName = jobConfiguration.fromJobConfig.datasetName;
    ftp = MainframeFTPClientUtils.getFTPConnection(context, linkConfiguration);
    if (ftp != null) {
      MainframeFTPClientUtils.setWorkingDirectory(context, ftp, parentDatasetName);
    }
  }

  @Override
  protected void close() throws IOException {
    if (datasetReader != null) {
      datasetReader.close();
    }
    if (ftp != null) {
      MainframeFTPClientUtils.closeFTPConnection(ftp);
    }
  }

  private String currentDatasetName = null;
  protected boolean getNextRecord() {
    String line = null;
    try {
      do {
        if (datasetReader == null) {
          currentDatasetName = getNextDataset();
          if (currentDatasetName == null) {
            break;
          }
          LOG.info("Processing dataset: " + currentDatasetName);
          InputStream in = ftp.retrieveFileStream(currentDatasetName);
          if (in == null) {
            throw new IOException("Unable to open file " + currentDatasetName + " in dataset " + parentDatasetName + " for ftp transfer. " + ftp.getReplyString());
          }
          datasetReader = new BufferedReader(new InputStreamReader(in));
        }
        line = datasetReader.readLine();
        if (line == null) {
          datasetReader.close();
          datasetReader = null;
          if (!ftp.completePendingCommand()) {
            throw new IOException("Failed to complete ftp command for data set " + currentDatasetName + ".");
          } else {
            LOG.info("Data transfer completed for data set " + currentDatasetName + ".");
          }
        }
      } while(line == null);
    } catch (IOException ioe) {
      try {
        close();
      }
      catch (IOException e) {
      }
      throw new SqoopException(MainframeConnectorError.GENERIC_MAINFRAME_CONNECTOR_0002, 
                    "IOException during data transfer: " + ioe.toString());
    }

    if (line != null) {
      setObjectArray(line);
      return true;
    }
    else {
      try {
        close();
      }
      catch(Exception e) {
        LOG.info("Exception while closing ftp connection. " + e);
      }
    }
    return false;
  }

}
