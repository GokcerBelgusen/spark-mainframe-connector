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
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfig;
import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.MainframeConnectorError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.schema.Schema;

/**
 * Extract from Mainframe.
 * Default field delimiter of a record is comma.
 */
public abstract class MainframeDatasetExtractor extends Extractor<LinkConfiguration, FromJobConfiguration, MainframeDatasetPartition> 
                      implements java.util.Iterator<Object[]> {

  public static final Logger LOG = Logger.getLogger(MainframeDatasetExtractor.class);

  private DataWriter dataWriter;
  private Schema schema;
  private long rowsRead = 0;
  private Object[] nextLine = null;
  private MainframeDatasetPartition partition;
  private boolean nextConsumed = true;
  private boolean nextExists;
  private int datasetProcessed;
  private long numberRecordRead = 0;

  private void initialize(ExtractorContext context, LinkConfiguration linkConfiguration, 
                          FromJobConfiguration jobConfiguration, MainframeDatasetPartition inputPartition) 
               throws IOException {
    numberRecordRead = 0;
    datasetProcessed = 0;
    partition = inputPartition;
    LOG.info("Working on partition: " + partition);
    schema = context.getSchema();
    initializeNextLineArray(schema);
    initializeConnection(context, linkConfiguration, jobConfiguration, inputPartition);
    nextExists = false;
  }

  protected abstract void initializeConnection(ExtractorContext context, LinkConfiguration linkConfiguration, 
                          FromJobConfiguration jobConfiguration, MainframeDatasetPartition inputPartition) 
               throws IOException;

  @Override
  public void extract(ExtractorContext context, LinkConfiguration linkConfiguration, FromJobConfiguration jobConfiguration, MainframeDatasetPartition inputPartition) {
    Iterator iter = getIterator(context, linkConfiguration, jobConfiguration, inputPartition);
    dataWriter = context.getDataWriter();
    while (iter.hasNext()) {
      Object[] nextLineArray = (Object[])(iter.next());
      dataWriter.writeArrayRecord(nextLineArray);
    }
  }

  public java.util.Iterator<Object[]>getIterator(ExtractorContext context, LinkConfiguration linkConfiguration,
                               FromJobConfiguration jobConfiguration, MainframeDatasetPartition inputPartition) {
    try {
      initialize(context, linkConfiguration, jobConfiguration, inputPartition);
      partition = inputPartition;
      return this;
    }
    catch (IOException e) {
      throw new SqoopException(MainframeConnectorError.GENERIC_MAINFRAME_CONNECTOR_0001, e.toString());
    }
  }

  protected String getNextDataset() {
    String datasetName = partition.getNextDataset();
    if (datasetName != null) {
      datasetProcessed++;
      LOG.info("Starting transfer of " + datasetName);
    }
    return datasetName;
  }

  @Override
  public boolean hasNext() {
    if (nextConsumed) {
      nextExists = getNextRecord();
      nextConsumed = false;
    }
    return nextExists;
  }

  @Override 
  public Object[] next() {
    nextConsumed = true;
    numberRecordRead++;
    return nextLine;
  }

  @Override 
  public void remove() {
  }

  protected abstract void close() throws IOException;

  protected abstract boolean getNextRecord();

  // This can be overridden by other classes
  protected void initializeNextLineArray(Schema schema) {
    nextLine = new Object[] {null}; // just initialize a one element array
  }

  // This can be overridden by other classes
  protected void setObjectArray(String line) {
    nextLine[0] = line;
  }

  @Override
  public long getRowsRead() {
    return numberRecordRead;
  }

}
