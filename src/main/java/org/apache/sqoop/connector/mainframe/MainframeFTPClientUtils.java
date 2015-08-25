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

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import org.apache.log4j.Logger;

import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.TransferableContext;


/**
 * Utility methods used when accessing a mainframe server through FTP client.
 */
public final class MainframeFTPClientUtils {
  private static final Logger LOG = Logger.getLogger(MainframeFTPClientUtils.class);

  private static FTPClient mockFTPClient = null; // Used for unit testing

  private MainframeFTPClientUtils() {
  }

  public static List<String> listSequentialDatasets(
      String pdsName, TransferableContext context, LinkConfiguration linkConfiguration) throws IOException {
    List<String> datasets = new ArrayList<String>();
    FTPClient ftp = null;
    try {
      ftp = getFTPConnection(context, linkConfiguration);
      if (ftp != null) {
        setWorkingDirectory(context, ftp, pdsName);
        FTPFile[] ftpFiles = ftp.listFiles();
        for (FTPFile f : ftpFiles) {
          if (f.getType() == FTPFile.FILE_TYPE) {
            datasets.add(f.getName());
          }
        }
      }
    } catch(IOException ioe) {
      throw new IOException ("Could not list datasets from " + pdsName + ":"
          + ioe.toString());
    } finally {
      if (ftp != null) {
        closeFTPConnection(ftp);
      }
    }
    return datasets;
  }

  public static FTPClient getFTPConnection(TransferableContext context, LinkConfiguration linkConfiguration)
      throws IOException {
    FTPClient ftp = null;
    try {
      String username = linkConfiguration.linkConfig.username;
      String password;
      if (username == null) {
        username = "anonymous";
        password = "";
      }
      else {
        password = linkConfiguration.linkConfig.password;
      }

      String connectString = linkConfiguration.linkConfig.uri;
      String server = connectString;
      int port = 0;
      String[] parts = connectString.split(":");
      if (parts.length == 2) {
        server = parts[0];
        try {
          port = Integer.parseInt(parts[1]);
        } catch(NumberFormatException e) {
          LOG.warn("Invalid port number: " + e.toString());
        }
      }

      if (null != mockFTPClient) {
        ftp = mockFTPClient;
      } else {
        ftp = new FTPClient();
      }

      // The following section to get the system key for FTPClientConfig is just there for testing purposes
      String systemKey = null;
      String systemTypeString = context.getString("spark.mainframe.connector.system.type", "MVS");
      if (systemTypeString.equals("MVS")) {
        systemKey = FTPClientConfig.SYST_MVS;
      }
      else if (systemTypeString.equals("UNIX")) {
        systemKey = FTPClientConfig.SYST_UNIX;
      }
      else {
        assert(false);
      }

      FTPClientConfig config = new FTPClientConfig(systemKey);
      ftp.configure(config);

      try {
        if (port > 0) {
          ftp.connect(server, port);
        } else {
          ftp.connect(server);
        }
      } catch(IOException ioexp) {
        throw new IOException("Could not connect to server " + server, ioexp);
      }

      int reply = ftp.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        throw new IOException("FTP server " + server
            + " refused connection:" + ftp.getReplyString());
      }
      LOG.info("Connected to " + server + " on " +
          (port>0 ? port : ftp.getDefaultPort()));
      if (!ftp.login(username, password)) {
        ftp.logout();
        throw new IOException("Could not login to server " + server
            + ":" + ftp.getReplyString());
      }
      // set ASCII transfer mode
      ftp.setFileType(FTP.ASCII_FILE_TYPE);
      // Use passive mode as default.
      ftp.enterLocalPassiveMode();
    } catch(IOException ioe) {
      if (ftp != null && ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch(IOException f) {
          // do nothing
        }
      }
      ftp = null;
      throw ioe;
    }
    return ftp;
  }

  public static void setWorkingDirectory(TransferableContext context, FTPClient ftp, String datasetName)
      throws IOException {
    String modifiedDatasetName = MainframeUtils.getDatasetName(context, datasetName);
    boolean replyCode = ftp.changeWorkingDirectory(modifiedDatasetName);
    if (!replyCode) {
      throw new IOException("Unable to change working directory to " + modifiedDatasetName + " for ftp transfer with ftp client = " + ftp + ". " + ftp.getReplyString());
    }
  }

  public static boolean closeFTPConnection(FTPClient ftp) {
    boolean success = true;
    try {
      ftp.noop(); // check that control connection is working OK
      ftp.logout();
    } catch(FTPConnectionClosedException e) {
      success = false;
      LOG.warn("Server closed connection: " + e.toString());
    } catch(IOException e) {
      success = false;
      LOG.warn("Server closed connection: " + e.toString());
    } finally {
      if (ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch(IOException f) {
          success = false;
        }
      }
    }
    return success;
  }

  // Used for testing only
  public static void setMockFTPClient(FTPClient FTPClient) {
    mockFTPClient = FTPClient;
  }

}
