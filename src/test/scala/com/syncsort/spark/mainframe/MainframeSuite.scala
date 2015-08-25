/*
 * Copyright 2014 Databricks
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
package com.syncsort.spark.mainframe

import java.io.FileNotFoundException
import java.io.File
import java.io.IOException
import java.sql.Timestamp
import java.util.{ArrayList, HashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import com.google.common.io.Files
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._

import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.mainframe.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.mainframe.configuration.LinkConfiguration;
import org.apache.sqoop.connector.mainframe.MainframeFTPClientUtils;
import org.apache.sqoop.job.etl.PartitionerContext;

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.mock._
import org.mockito.Mockito._

import scala.util.Random

/* Implicits */
import com.syncsort.spark.mainframe.connector.mainframe._
import TestSQLContext._

class MainframeSuite extends FunSuite with BeforeAndAfter with MockitoSugar {

  test("no user specified schemas") {
    try {
      sql(
        s"""
          |CREATE TEMPORARY TABLE mainframeTable (carMake string)
          |USING com.syncsort.spark.mainframe.connector
          |OPTIONS (username "name", password "pw", uri "u", datasetName "d")
        """.stripMargin.replaceAll("\n", " "))
    }
    catch {
      case e : RuntimeException => {
        assert(e.toString.indexOf("does not allow user-specified schemas.") != -1)
      }
    }
  }

  test("check default schema") {
    val all = TestSQLContext.load("com.syncsort.spark.mainframe.connector", Map("connectorClass" -> "mainframe", "username" -> "username", "password" -> "password", "uri" -> "uri", "datasetName" -> "datasetName"))    
    assert(all.toString.equals("[record: string]"))
  }

  test("default connector is mainframe") {
    val all = TestSQLContext.load("com.syncsort.spark.mainframe.connector", Map("username" -> "username", "password" -> "password", "uri" -> "uri", "datasetName" -> "datasetName"))    
    assert(all.toString.equals("[record: string]"))
  }

  test("only mainframe class supported") {
    try {
      val all = TestSQLContext.load("com.syncsort.spark.mainframe.connector", Map("connectorClass" -> "xyz", "username" -> "username", "password" -> "password", "uri" -> "uri", "datasetName" -> "datasetName"))    
    }
    catch {
      case e : RuntimeException => {
        assert(e.toString.indexOf("Connector class xyz is not supported") != -1)
      }
    }
  }

  test("Could not connect to server") {
    val mockFTPClient = mock[FTPClient]
    when(mockFTPClient.getReplyString()).thenReturn("Forcing connection to fail.");
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
    val username = "testUser"
    val password = "testPW"
    val uri = "uri"
    val datasetName = "xxx"
    try {
      when(mockFTPClient.connect(uri)).thenThrow(new IOException("Forcing connection to fail."))
      sql(
        s"""
          |CREATE TEMPORARY TABLE mainframeTable 
          |USING com.syncsort.spark.mainframe.connector
          |OPTIONS (username '$username', password '$password', uri '$uri', datasetName '$datasetName')
        """.stripMargin.replaceAll("\n", " "))
      val readEntries = sql("SELECT record FROM mainframeTable").collect
    } 
    catch {
      case sqe : SqoopException => {
        assert(sqe.toString.indexOf(s"Could not connect to server $uri") != -1);
      }
    }
  }

  test("Connection refused") {
    val mockFTPClient = mock[FTPClient]
    when(mockFTPClient.getReplyString()).thenReturn("");
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
    val username = "testUser"
    val password = "testPW"
    val uri = "uri"
    val datasetName = "xxx"
    try {
      when(mockFTPClient.getReplyCode()).thenReturn(FTPReply.REQUEST_DENIED)
      when(mockFTPClient.login(username, password)).thenReturn(false);
      sql(
        s"""
          |CREATE TEMPORARY TABLE mainframeTable 
          |USING com.syncsort.spark.mainframe.connector
          |OPTIONS (username '$username', password '$password', uri '$uri', datasetName '$datasetName')
        """.stripMargin.replaceAll("\n", " "))
      val readEntries = sql("SELECT record FROM mainframeTable").collect
    } 
    catch {
      case sqe : SqoopException => {
        assert(sqe.toString.indexOf(s"FTP server $uri refused connection") != -1);
      }
    }
  }

  test("login failed") {
    val mockFTPClient = mock[FTPClient]
    when(mockFTPClient.getReplyString()).thenReturn("");
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
    val username = "testUser"
    val password = "testPW"
    val uri = "uri"
    val datasetName = "testDataset"
    try {
      when(mockFTPClient.getReplyCode()).thenReturn(FTPReply.COMMAND_OK)
      when(mockFTPClient.login(username, password)).thenReturn(false);
      sql(
        s"""
          |CREATE TEMPORARY TABLE mainframeTable 
          |USING com.syncsort.spark.mainframe.connector
          |OPTIONS (username '$username', password '$password', uri '$uri', datasetName '$datasetName')
        """.stripMargin.replaceAll("\n", " "))
      val readEntries = sql("SELECT record FROM mainframeTable").collect
    } 
    catch {
      case sqe : SqoopException => {
        assert(sqe.toString.indexOf(s" Could not login to server $uri:") != -1);
      }
    }
  }

  test("Dataset does not exist") {
    val mockFTPClient = mock[FTPClient]
    when(mockFTPClient.getReplyString()).thenReturn("");
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
    val username = "testUser"
    val password = "testPW"
    val uri = "uri"
    val datasetName = "xxx"
    try {
      when(mockFTPClient.getReplyCode()).thenReturn(FTPReply.COMMAND_OK)
      when(mockFTPClient.login(username, password)).thenReturn(true)
      when(mockFTPClient.changeWorkingDirectory("'" + datasetName + "'")).thenReturn(false)
      sql(
        s"""
          |CREATE TEMPORARY TABLE mainframeTable 
          |USING com.syncsort.spark.mainframe.connector
          |OPTIONS (username '$username', password '$password', uri '$uri', datasetName '$datasetName')
        """.stripMargin.replaceAll("\n", " "))
      val readEntries = sql("SELECT record FROM mainframeTable").collect
    } 
    catch {
      case sqe : SqoopException => {
        assert(sqe.toString.indexOf(s"Could not list datasets from $datasetName:java.io.IOException: Unable to change working directory to '$datasetName'") != -1);
      }
    }
  }

  private def mockInputDataset : scala.collection.mutable.MutableList[String] = {
      val dataset = "testDataset"
      val mockDataset = "src/test/resources/" + dataset
      val mockFTPClient = mock[FTPClient]
      when(mockFTPClient.getReplyString()).thenReturn("")
      MainframeFTPClientUtils.setMockFTPClient(mockFTPClient)
      when(mockFTPClient.login("testUser", "testPW")).thenReturn(true)
      when(mockFTPClient.changeWorkingDirectory("'" + dataset + "'")).thenReturn(true)
      when(mockFTPClient.getReplyCode()).thenReturn(FTPReply.COMMAND_OK)
      // create mock FTPFile list using directory listing of dataset
      // for each FTPFile, set name and type
      // for listFiles below, return the array created above
      val datasetDir = new java.io.File(mockDataset)
      val fileArray = datasetDir.listFiles
      val ftpFileArray = new Array[FTPFile](fileArray.length)
      var i : Int = 0
      var fileEntries = new scala.collection.mutable.MutableList[String]
      for (file <- fileArray) {
        val ftpFile = new FTPFile
        ftpFile.setName(file.getName)
        ftpFile.setType(FTPFile.FILE_TYPE)
        ftpFileArray(i) = ftpFile
        for (line <- scala.io.Source.fromFile(mockDataset + "/" + file.getName).getLines) {
          fileEntries += line
        }
        when(mockFTPClient.retrieveFileStream(file.getName))thenReturn(new java.io.FileInputStream(file))
        i += 1
      }
      val sortedFileEntries = fileEntries.sortWith(_ < _)
      when(mockFTPClient.listFiles()).thenReturn(ftpFileArray)
      when(mockFTPClient.completePendingCommand()).thenReturn(true)
      sortedFileEntries
  }

  private def verifyEntries(readEntries : Array[Row], sortedEntries : scala.collection.mutable.MutableList[String]) {
    var j : Int = 0
    for (entry <- sortedEntries) {
      assert(entry === readEntries(j).get(0).asInstanceOf[String])
      j += 1
    }
  }

  test("read 6 files in 3 partitions Using sql") {
    try {
      val sortedFileEntries = mockInputDataset
      sql(
        s"""
          |CREATE TEMPORARY TABLE mainframeTable 
          |USING com.syncsort.spark.mainframe.connector
          |OPTIONS (username "testUser", password "testPW", uri "uri", datasetName "testDataset")
        """.stripMargin.replaceAll("\n", " "))
      sql("CACHE TABLE mainframeTable")
      val readEntries = sql("SELECT record FROM mainframeTable ORDER BY record").collect
      verifyEntries(readEntries, sortedFileEntries)
    }
    catch {
      case e: Exception => {
        println("Unexpected exception " + e)
        e.printStackTrace()
        assert(false)
      }
    }    
  }

  test("read 6 files in 3 partitions using mainframeDataset method") {
    try {
      val sortedFileEntries = mockInputDataset
      val input3 = TestSQLContext.mainframeDataset(Map("username" -> "testUser", "password" -> "testPW", "uri" -> "uri", "datasetName" -> "testDataset"))
      val readEntries = input3.sort("record").collect
      verifyEntries(readEntries, sortedFileEntries)
    }
    catch {
      case e: Exception => {
        println("Unexpected exception " + e)
        e.printStackTrace()
        assert(false)
      }
    }    
  }

}
