# Spark Mainframe Library

A library for accessing Mainframe data with Apache Spark using Spark SQL and DataFrames.

## Requirements

This library requires Spark 1.3+

## Building from Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file, simply run `sbt/sbt package` from the project root.

Tests can be run as: `sbt/sbt test`

## Using with Spark shell
After building the package, it can be added to Spark using the `--jars` command line option. Two additional jar files
that are provided in the lib directory need to be specified in the `--driver-class-path` option as well.
For example, assuming that $SPARK_MAINFRAME_PATH is where this package has been downloaded:

```
SPARK_MAINFRAME_CONNECTOR_CLASSPATH=$SPARK_MAINFRAME_PATH/target/scala-2.10/spark-mainframe-connector_2.10-1.0.0.jar:SPARK_MAINFRAME_PATH/lib/connector-sdk-1.99.6.jar:SPARK_MAINFRAME_PATH/lib/sqoop-common-1.99.6.jar 
$ bin/spark-shell --jars $SPARK_MAINFRAME_CONNECTOR_CLASSPATH --driver-class-path $SPARK_MAINFRAME_CONNECTOR_CLASSPATH
    
```

## Features
This package allows importing all sequential datasets in a partitioned dataset (PDS) 
on a mainframe as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
A PDS is akin to a directory on open systems. The records in a dataset can contain only character data.

When reading datasets, the API requires the following options to be specified:
* `datasetName`: the location of the dataset on the mainframe, using the full path; globbing expressions are not accepted
* `username`: the username that will be used to log into the mainframe to access the dataset
* `password`: the password for the specified username to log into the mainframe
* `uri`: the name of the mainframe
* `numPartitions` : (optional) the number of partitions to be created by the underlying RDD. This will be the maximum number of concurrent connections opened to the mainframe. All sequential datasets in the partitioned dataset will be available in the Dataframe. If not specified, the default is 10.
All sequential datasets in the partitioned dataset will be available in the Dataframe.

The package does not support saving Dataframes to the mainframe.

### SQL API
Mainframe data can be queried in pure SQL by registering the data as a (temporary) table.

```sql
CREATE TEMPORARY TABLE sales
USING com.syncsort.spark.mainframe.connector
OPTIONS (datasetName "ABCD.SALESDETAILS", username "johndoe", password "dfg43%", uri "accounts.example.com")
```

This table has only one column named 'record', of type string.
It's not possible to specify an alternative schema at the time of creating the dataframe. However, if 
the schema is known, a new dataframe can be created using a SELECT statement with the 'AS' keyword.

```scala
val salesinput = sqlContext.sql(s"CREATE TEMPORARY TABLE salesinput USING com.syncsort.spark.mainframe.connector OPTIONS (username 'user', password 'pw1', uri 'abc.xyz.com', datasetName 'salesdetails', numPartitions "6")")

// create a new dataframe and specify a schema for the rows
val sales = sqlContext.sql("SELECT SUBSTRING(record, 1, 4) AS saleID, SUBSTRING(record, 6, 10) AS sdate, SUBSTRING(record, 17, 5) AS stime, SUBSTRING(record, 23, 4) AS salesperson_ID, SUBSTRING(record, 28, 5) AS custID, SUBSTRING(record, 35, 8) AS amount FROM salesinput")
```

### Scala API

Using DataFrame APIs:

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load("com.syncsort.spark.mainframe.connector", Map("username" -> "johndoe", "password" -> "dfg43%", "uri" -> "accounts.example.com", "datasetName" -> "salesdetails", "numPartitions" -> "6"))
df.select("record").filter($"record" > "2015-02-28")
```

Using mainframeDataset method:

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import com.syncsort.spark.mainframe.connector_

// For implicits
import com.syncsort.spark.mainframe.connector.mainframe._
import sqlContext._
import sqlContext.implicits._

val salesDetails = sqlContext.mainframeDataset(Map("username" -> "johndoe", "password" -> "dfg43%", "uri" -> "accounts.example.com", "datasetName" -> "salesdetails", "numPartitions" -> "6"))

salesDetails.registerTempTable("mainframedataset")
sqlContext.cacheTable("mainframedataset")
salesDetails.collect().foreach(line => println(line))
salesDetails.save(outputFile)
println("count = " + salesDetails.count())

```

## License

Copyright 2015, Syncsort Incorporated

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
