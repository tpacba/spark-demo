package com.spark.scala.learning

import org.apache.spark.sql.{SaveMode, SparkSession}

object IngestionFromOracle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OracleIngester").enableHiveSupport().getOrCreate()

    val data_spotify_artists = spark.read.format("jdbc")
      .option("url","jdbc:oracle:thin:@localhost:1521:xe")
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("dbtable","spotify_artists")
      .option("user","hr")
      .option("password","hr")
      .load()
    val data_youtube_creators = spark.read.format("jdbc")
      .option("url","jdbc:oracle:thin:@localhost:1521:xe")
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("dbtable","youtube_creators")
      .option("user","hr")
      .option("password","hr")
      .load()

    data_spotify_artists.createOrReplaceTempView("temp_spotify_artists")
    data_youtube_creators.createOrReplaceTempView("temp_youtube_creators")

    val df_spotify_artists = spark.table("temp_spotify_artists")
    val df_youtube_creators = spark.table("temp_youtube_creators")

    df_spotify_artists.show()
    df_youtube_creators.show()

    df_spotify_artists.write.mode(SaveMode.Overwrite).format("csv").save("/demo/spotify_artists.csv")
    df_spotify_artists.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("market.spotify_artists")
    df_youtube_creators.write.mode(SaveMode.Overwrite).format("csv").save("/demo/youtube_creators.csv")
    df_youtube_creators.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("market.youtube_creators")

    spark.stop()

  }
}
