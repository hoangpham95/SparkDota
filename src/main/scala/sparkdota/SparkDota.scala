package sparkdota

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDota {
  val dataPath =
    "/home/hpham/Desktop/dota.json" //"s3a://emrfs-dota-data/yasp-dump.json"

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Spark Dota")
      .config("spark.master", "local")
      .getOrCreate()
// implicit conversions
  import spark.implicits._
  val sc: SparkContext = spark.sparkContext
  val hc = sc.hadoopConfiguration
  val awsCred = getAWSCred();

  // println(awsCred)

  hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hc.set("fs.s3a.awsAccessKeyId", awsCred(1))
  hc.set("fs.s3a.awsSecretAccessKey", awsCred(2))

  def main(args: Array[String]): Unit = {
    processData();
    spark.stop()
  }

  def processData() {
    val rawDataRDD = sc
      .wholeTextFiles(dataPath)
      .map(_._2.replace("\n", "").trim)

    val df = spark.sqlContext.read.json(rawDataRDD)
    val filtered = df
      .select($"game_mode", $"players", $"radiant_win")
      .where($"human_players" === 10)
      .where($"game_mode".isin("1", "2", "22"))
      .where(!array_contains($"players.leaver_status", 1))

    filtered.show()
  }

  def getAWSCred(): List[String] = {
    try {
      val filename = "src/main/resources/sparkdota/rootkey.txt"
      Source.fromFile(filename).getLines.toList
    } catch {
      case e => println("Path to the rootkey is not correct")
    }
  }
}
