package sparkdota

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Player(
    account_id: Long,
    player_slot: Long,
    hero_id: Long,
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
    players: Seq[Player],
    radiant_win: Boolean
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
  val awsCred = getAWSCred();

  hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hc.set("fs.s3a.awsAccessKeyId", awsCred(0))
  hc.set("fs.s3a.awsSecretAccessKey", awsCred(1))

  def main(args: Array[String]): Unit = {
    processData();
    spark.stop()
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

    firstRound.printSchema()
    firstRound.show();

    // val secondRound = firstRound
    //   .map(
    //     mat =>
    //       Match(
    //         mat.getLong(0), // game_mode
    //         mat.getLong(1), // match_id
    //         mat.getLong(2), // match_seq_num
    //         mat.getLong(3), // duration
    //         mat
    //           .getAs[Seq[Row]](5)
    //           .map(p =>
    //             Player(
    //               p.getLong(2), // account_id
    //               p.getLong(41), // player_slot
    //               p.getLong(19), // hero_id,
    //               p.getLong(29), // kills
    //               p.getLong(9), // deaths
    //               p.getLong(4), // assists,
    //               p.getLong(33) == 1, // leaver_status
    //               p.getLong(32), // last_hits
    //               p.getLong(12), // gold_per_min
    //               p.getLong(49), // xp_per_min
    //               p.getLong(16), // hero_damage
    //               p.getLong(48) // tower_damange
    //           )),
    //         mat.getBoolean(4) // radiant_win
    //     ))

    // secondRound.printSchema();
    // secondRound.show();
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
