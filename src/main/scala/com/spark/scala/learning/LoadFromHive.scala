package com.spark.scala.learning

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadFromHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveLoader").enableHiveSupport().getOrCreate()

    val df_join_user_socials = spark.table("market.join_user_socials")
    val df_join_artist_events = spark.table("market.join_artist_events")
    val df_join_creator_events = spark.table("market.join_creator_events")

    df_join_user_socials.show()
    df_join_artist_events.show()
    df_join_creator_events.show()

    df_join_user_socials.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url","jdbc:sqlite:/home/fieldemployee/Desktop/demo.db")
      .option("driver","org.sqlite.JDBC")
      .option("dbtable","join_user_socials")
      .save()
    df_join_artist_events.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url","jdbc:sqlite:/home/fieldemployee/Desktop/demo.db")
      .option("driver","org.sqlite.JDBC")
      .option("dbtable","join_artist_events")
      .save()
    df_join_creator_events.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url","jdbc:sqlite:/home/fieldemployee/Desktop/demo.db")
      .option("driver","org.sqlite.JDBC")
      .option("dbtable","join_creator_events")
      .save()

    spark.stop()
  }
}
