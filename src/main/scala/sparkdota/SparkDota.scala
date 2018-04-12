package sparkdota

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import play.api.libs.json._
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class Player(
    hero_id: Long,
    radiant: Boolean
)

case class Match(
    radiant_win: Boolean,
    radiant: Seq[Long],
    dire: Seq[Long]
)

object SparkDota {
  val dataPath = "s3://emrfs-dota-data/yasp-dump.json"
  // val dataPath = "/home/hoang/Downloads/dota.json"

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Spark Dota")
      .config("spark.master", "local[*]")
      .getOrCreate()

// implicit conversions
  import spark.implicits._
  
  val sc: SparkContext = spark.sparkContext
  val hc = sc.hadoopConfiguration
  val awsCred = getAWSCred()
  val stream = getClass.getResourceAsStream("/hero.json")
  val heroDataSource = Source.fromInputStream(stream);

  def main(args: Array[String]): Unit = {
    // getHeroId().foreach(println)
    // processData();
    processMatchData("/home/hpham/match_data.csv")
    processLosingAgainst("/home/hpham/match_data.csv")
    // heroDataSource.close()
    spark.stop()
  }

  def getHeroId(): List[Int] = {
    case class Hero(
      localized_name: String,
      url_full_portrait: String,
      name: String,
      url_small_portrait: String,
      url_large_portrait: String,
      url_vertical_portrait: String,
      id: Int
    )

    implicit val heroReads: Reads[Hero] = (
      (JsPath \ "localized_name").read[String] and
      (JsPath \ "url_full_portrait").read[String] and
      (JsPath \ "name").read[String] and
      (JsPath \ "url_small_portrait").read[String] and
      (JsPath \ "url_large_portrait").read[String] and
      (JsPath \ "url_vertical_portrait").read[String] and
      (JsPath \ "id").read[Int]
    )(Hero.apply _)

    val json = Json.parse(heroDataSource.getLines.mkString);
    val heroes = json("heroes").as[List[Hero]]

    heroes.map(_.id).sorted
  }

  def processData() {
    // val jsonRDD = sc.textFile()

    val df = spark
      .read
      .option("mode", "PERMISSIVE")
      .option("multiline", true)
      // .option("samplingOption", 0.05)
      .json(dataPath)

    val firstRound = df
      .select(
        $"radiant_win",
        // parsing players data
        $"players.hero_id".as("hero_id"),
        $"players.player_slot".as("player_slot")
      )
      .where($"human_players" === 10)
      .where($"game_mode".isin("1", "2", "22"))
      .where(!array_contains($"players.leaver_status", 1))
      .map((r: Row) => {
          val playerList = convertToPlayerList(r.getAs[Seq[Long]](1), r.getAs[Seq[Long]](2)).sortBy(_.hero_id)
          Match(
            r.getAs[Boolean](0),
            playerList.filter(_.radiant).map(_.hero_id),
            playerList.filter(!_.radiant).map(_.hero_id)
          )
        }
      )
    val time = System.currentTimeMillis().toString()
    val stringify = udf((vs: Seq[Long]) => vs.mkString(","))
    // val filePath = "/home/hoang/Desktop/spark-output-"
    val filePath = "s3://emrfs-dota-data/spark-output-"
    firstRound.withColumn("radiant", stringify($"radiant"))
      .withColumn("dire", stringify($"dire"))
      .write.format("csv").save(filePath + time)
  }

  def convertToPlayerList(
    heroIds: Seq[Long],
    playerSlots: Seq[Long]
  ): Seq[Player] = {
    for {i <- 0 to 9}
      yield Player(
        heroIds(i), 
        playerSlots(i) / 128 == 0
      )
  }

  def getAWSCred(): List[String] = {
    try {
      val filename = "src/main/resources/sparkdota/rootkey.txt"
      Source.fromFile(filename).getLines.toList
    } catch {
      case _: Exception => List()
    }
  }

  def getWinCouple(s: Seq[Long], isWin: Boolean): List[(Int, Int, Boolean)] = {
    val seq = for {
      i <- 0 to s.length - 1
      j <- i + 1 to s.length - 1
    }
      yield (Math.min(s(i).toInt, s(j).toInt), 
            Math.max(s(i).toInt, s(j).toInt), 
            isWin)    

    seq.toList
  }

  def processMatchData(datapath: String = "s3://emrfs-dota-data/match_data.csv") = {
    val df = spark.read.format("csv")
      .option("header", true)
      .load(datapath)
      .select($"radiant_win", $"radiant", $"dire")
      .map((r: Row) => Match(r.getAs[String](0) == "True", r.getAs[String](1).split(",").map(_.toLong), r.getAs[String](2).split(",").map(_.toLong)))

    val matchRDD: RDD[Match] = df.rdd

    val winCouples = matchRDD.flatMap(r => {
      val w = if (r.radiant_win) r.radiant else r.dire
      val l = if (r.radiant_win) r.dire else r.radiant

      val win: List[(Int, Int, Boolean)] = getWinCouple(w, true)
      val lose: List[(Int, Int, Boolean)] = getWinCouple(l, false)
      
      assert(win.length == 10)
      assert(lose.length == 10)

      win ::: lose
    })
    .map(r => ((r._1, r._2), (if (r._3) 1 else 0, if (r._3) 0 else 1))) // RDD[((Int, Int), (Int, Int))]
    .reduceByKey((acc, m2) => (acc._1 + m2._1, acc._2 + m2._2))
    .mapValues(rec => (rec._1.toFloat / (rec._1 + rec._2)))
    .map(heRec => (heRec._1._1, heRec._1._2, heRec._2))
    .toDF.write.csv("/home/hpham/win.csv")
  }

  def processLosingAgainst(datapath: String = "s3://emrfs-dota-data/match_data.csv") = {
    val df = spark.read.format("csv")
      .option("header", true)
      .load(datapath)
      .select($"radiant_win", $"radiant", $"dire")
      .map((r: Row) => Match(r.getAs[String](0) == "True", r.getAs[String](1).split(",").map(_.toLong), r.getAs[String](2).split(",").map(_.toLong)))

    
    val matchRDD: RDD[Match] = df.rdd

    val losingRate = matchRDD.flatMap(m => {
      val w = if (m.radiant_win) m.radiant else m.dire
      val l = if (m.radiant_win) m.dire else m.radiant

      for {
        i <- 0 to w.length - 1
        j <- 0 to l.length - 1
      }
        yield List((w(i), l(j), true), (l(j), w(i), false))
    })
    .flatMap(x => x)
    .map(r => ((r._1, r._2), (if (r._3) 1 else 0, if (r._3) 0 else 1)))
    .reduceByKey((acc, m2) => (acc._1 + m2._1, acc._2 + m2._2))
    .mapValues(rec => ((rec._1.toFloat/(rec._1 + rec._2)), ((rec._2.toFloat/(rec._1 + rec._2)))))
    .map(r => (r._1._1, r._1._2, r._2._1, r._2._2))
    .toDF.write.csv("/home/hpham/lose.csv")
  }
}
