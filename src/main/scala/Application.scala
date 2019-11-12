import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.io.Source

object Application extends App {
  val spark = SparkSession
    .builder()
    .appName("SparkAndDatabases")
    .config("spark.master", "local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  val url = "jdbc:postgresql://127.0.0.1:5432/test_dataset"
  val tableName: String = "beijing_taxi"
  val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("driver", "org.postgresql.Driver")

  val USER_HOME_DIR = System.getProperty("user.home")
  val sourceDataPath = s"$USER_HOME_DIR/Downloads/release/taxi_log_2008_by_id"

  val listOfFiles: List[String] = Files.walk(Paths.get(sourceDataPath))
    .iterator()
    .asScala
    .filter(Files.isRegularFile(_))
    .map(_.toString)
    .toList

  import spark.implicits._
  // Approach 1 (write content of each file to database sequentially)
  writeSequentially(listOfFiles)
  // Approach 2 (concat content of each file parallel and write all data to database in one operation)
  writeParallel(listOfFiles)

  // Loading data from a JDBC source
  val jdbcDF: DataFrame = spark.read.jdbc(url, tableName, connectionProperties)
  println(jdbcDF.show)

  def writeSequentially(listOfFiles: List[String]) = {
    val startTime: Long = System.nanoTime()
    listOfFiles.foreach(fileDir => {
      val positionsDF: DataFrame = spark.sparkContext
        .textFile(fileDir)
        .map(row => row.split(","))
        .map(row => new Position(row(0).toInt, Timestamp.valueOf(row(1)), row(2).toDouble, row(3).toDouble))
        .toDF()

      positionsDF.write
        .mode(SaveMode.Append)
        .jdbc(url, tableName, connectionProperties)
    })
  }

  def writeParallel(listOfFiles: List[String]) = {
    val startTime: Long = System.nanoTime()
    
    val taxiData: RDD[Position] = spark.sparkContext.parallelize(listOfFiles)
      .repartition(2)
      .mapPartitions(partition => {
        partition.map(fileDir => {
          Source.fromFile(new File(fileDir))
            .getLines
            .toList
            .map(row => row.split(","))
            .map(row => Position(row(0).toInt, Timestamp.valueOf(row(1)), row(2).toDouble,  row(3).toDouble))
        })
      }).flatMap(x => x)

    val elapsedTime: Long = (System.nanoTime() - startTime)
    println(s"time of execution = $elapsedTime ns")

    val startTime2: Long = System.nanoTime()
    
    taxiData.toDF()
      .write
      .mode(SaveMode.Append)
      .jdbc(url, tableName, connectionProperties)

    val elapsedTime2: Long = (System.nanoTime() - startTime2)
    println(s"time of execution = $elapsedTime2 ns")
  }
}
