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
    account_id: Long,
    hero_id: Long,
    player_slot: Long,
    kills: Long,
    deaths: Long,
    assists: Long,
    leaver_status: Boolean,
    last_hits: Long,
    gold_per_min: Long,
    xp_per_min: Long,
    hero_damage: Long,
    tower_damange: Long
)

case class Match(
    game_mode: Long,
    match_id: Long,
    match_seq_num: Long,
    duration: Long,
    radiant_win: Boolean,
    players: Seq[Player]
)

object SparkDota {
  val dataPath = "s3a://emrfs-dota-data/yasp-dump.json"

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
  val awsCred = getAWSCred()
  val heroPath = "src/main/resources/sparkdota/hero.json"
  val heroDataSource = Source.fromFile(heroPath);

  hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hc.set("fs.s3a.awsAccessKeyId", awsCred(0))
  hc.set("fs.s3a.awsSecretAccessKey", awsCred(1))

  def main(args: Array[String]): Unit = {
    getHeroId().foreach(println)
    // processData();
    heroDataSource.close()
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
    val rawDataRDD = sc
      .textFile(dataPath)
      .take(5)

    val data = sc.parallelize(rawDataRDD)

    val df = spark.sqlContext.read.json(data)

    val firstRound = df
      .select(
        $"game_mode",
        $"match_id",
        $"match_seq_num",
        $"duration",
        $"radiant_win",
        // parsing players data
        $"players.account_id".as("account_id"),
        $"players.hero_id".as("hero_id"),
        $"players.player_slot".as("player_slot"),
        $"players.kills".as("kills"),
        $"players.deaths".as("deaths"),
        $"players.assists".as("assists"),
        $"players.leaver_status".as("leaver_status"),
        $"players.last_hits".as("last_hits"),
        $"players.gold_per_min".as("gold_per_min"),
        $"players.xp_per_min".as("xp_per_min"),
        $"players.hero_damage".as("hero_damage"),
        $"players.tower_damage".as("tower_damage")
      )
      .where($"human_players" === 10)
      .where($"game_mode".isin("1", "2", "22"))
      .where(!array_contains($"leaver_status", 1))
      .map((r: Row) => Match(
        r.getAs[Long](0),
        r.getAs[Long](1),
        r.getAs[Long](2),
        r.getAs[Long](3),
        r.getAs[Boolean](4),
        convertToPlayerList(
          r.getAs[Seq[Long]](5),
          r.getAs[Seq[Long]](6),
          r.getAs[Seq[Long]](7),
          r.getAs[Seq[Long]](8),
          r.getAs[Seq[Long]](9),
          r.getAs[Seq[Long]](10),
          r.getAs[Seq[Long]](11),
          r.getAs[Seq[Long]](12),
          r.getAs[Seq[Long]](13),
          r.getAs[Seq[Long]](14),
          r.getAs[Seq[Long]](15),
          r.getAs[Seq[Long]](16)
        )
      ))

    firstRound.printSchema()
    firstRound.show();
  }

  def convertToPlayerList(
    accountIds: Seq[Long],
    heroIds: Seq[Long],
    playerSlots: Seq[Long],
    kills: Seq[Long],
    deaths: Seq[Long],
    assists: Seq[Long],
    leaverStats: Seq[Long],
    lastHits: Seq[Long],
    gpm: Seq[Long],
    xpm: Seq[Long],
    dmgs: Seq[Long],
    towerDmg: Seq[Long]
  ): Seq[Player] = {
    for {i <- 0 to 9}
      yield Player(
        accountIds(i), 
        heroIds(i), 
        playerSlots(i), 
        kills(i), 
        deaths(i),
        assists(i),
        leaverStats(i) == 1,
        lastHits(i),
        gpm(i),
        xpm(i),
        dmgs(i),
        towerDmg(i)
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
