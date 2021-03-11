package com.spark.scala.learning

import org.apache.spark.sql.SparkSession

object CreateHiveTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExternalTable").enableHiveSupport().getOrCreate()

    spark.sql("use market;")

    spark.sql("drop table if exists join_user_socials;")
    spark.sql("create table join_user_socials(id int,user_id int,first_name string,last_name string,city string,state string,country string,social_site string,username_used string,email_used string);")
    spark.sql("insert into table join_user_socials select id,user_socials.user_id,first_name,last_name,city,state,country,social_site,username_used,email_used from user_socials left join users on user_socials.user_id=users.user_id;")

    spark.sql("drop table if exists join_artist_events;")
    spark.sql("create table join_artist_events(event_id int, event_name string, event_date date, artist_id int, name string, city string, state string, country string);")
    spark.sql("insert into table join_artist_events select event_id,event_name,event_date,artist_events.artist_id,name,city,state,country from artist_events left join spotify_artists on artist_events.artist_id=spotify_artists.artist_id;")

    spark.sql("drop table if exists join_creator_events;")
    spark.sql("create table join_creator_events(event_id int, event_name string, event_date date, creator_id int, username string, city string, state string, country string);")
    spark.sql("insert into table join_creator_events select event_id,event_name,event_date,creator_events.creator_id,username,city,state,country from creator_events left join youtube_creators on creator_events.creator_id=youtube_creators.creator_id;")

    spark.stop()
  }
}
