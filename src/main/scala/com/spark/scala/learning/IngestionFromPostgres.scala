package com.spark.scala.learning

import org.apache.spark.sql.{SaveMode, SparkSession}

object IngestionFromPostgres {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PostgresIngester").enableHiveSupport().getOrCreate()

    val data_artist_events = spark.read.format("jdbc")
      .option("url","jdbc:postgresql://localhost:5432/postgres")
      .option("driver","org.postgresql.Driver")
      .option("dbtable","artist_events")
      .option("user","hr")
      .option("password","hr")
      .load()

    val data_creator_events = spark.read.format("jdbc")
      .option("url","jdbc:postgresql://localhost:5432/postgres")
      .option("driver","org.postgresql.Driver")
      .option("dbtable","creator_events")
      .option("user","hr")
      .option("password","hr")
      .load()

    data_artist_events.createOrReplaceTempView("temp_artist_events")
    data_creator_events.createOrReplaceTempView("temp_creator_events")

    val df_artist_events = spark.table("temp_artist_events")
    val df_creator_events = spark.table("temp_creator_events")

    df_artist_events.show()
    df_creator_events.show()

    df_artist_events.write.mode(SaveMode.Overwrite).format("csv").save("/demo/artist_events.csv")
    df_artist_events.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("market.artist_events")
    df_creator_events.write.mode(SaveMode.Overwrite).format("csv").save("/demo/creator_events.csv")
    df_creator_events.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("market.creator_events")

    spark.stop()
  }
  }
