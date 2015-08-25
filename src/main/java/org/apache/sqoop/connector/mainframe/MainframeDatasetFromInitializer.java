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

import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;

public class MainframeDatasetFromInitializer extends Initializer<LinkConfiguration, FromJobConfiguration> {

  private static final Logger LOG =
    Logger.getLogger(MainframeDatasetFromInitializer.class);

  @Override
  public void initialize(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
  }

  @Override
  public Set<String> getJars(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    Set<String> jars = super.getJars(context, linkConfig, fromJobConfig);
    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    return null;
  }

}
