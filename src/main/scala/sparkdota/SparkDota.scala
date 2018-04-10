package sparkdota

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
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
    match_id: Long,
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
      .getOrCreate()

// implicit conversions
  import spark.implicits._
  
  val sc: SparkContext = spark.sparkContext
  val hc = sc.hadoopConfiguration
  // val awsCred = getAWSCred()
  val stream = getClass.getResourceAsStream("/hero.json")
  val heroDataSource = Source.fromInputStream(stream);

  def main(args: Array[String]): Unit = {
    // getHeroId().foreach(println)
    processData();
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
    val df = spark
      .read
      .option("mode", "PERMISSIVE")
      .option("multiline", true)
      .json(dataPath)

    val firstRound = df
      .select(
        $"match_id",
        $"radiant_win",
        // parsing players data
        $"players.hero_id".as("hero_id"),
        $"players.player_slot".as("player_slot")
      )
      .where($"human_players" === 10)
      .where($"game_mode".isin("1", "2", "22"))
      .where(!array_contains($"players.leaver_status", 1))
      .map((r: Row) => {
          val playerList = convertToPlayerList(r.getAs[Seq[Long]](2), r.getAs[Seq[Long]](3)).sortBy(_.hero_id)
          Match(
            r.getAs[Long](0),
            r.getAs[Boolean](1),
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
}
