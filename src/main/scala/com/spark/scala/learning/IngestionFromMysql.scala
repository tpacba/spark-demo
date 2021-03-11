package com.spark.scala.learning

import org.apache.spark.sql.{SaveMode, SparkSession}

object IngestionFromMysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MysqlIngester").enableHiveSupport().getOrCreate()

    val data_users = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost/marketing")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","users")
      .option("user","root")
      .option("password","root")
      .load()

    val data_user_socials = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost/marketing")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","user_socials")
      .option("user","root")
      .option("password","root")
      .load()

    data_users.createOrReplaceTempView("temp_users")
    data_user_socials.createOrReplaceTempView("temp_user_socials")

    val df_users = spark.table("temp_users")
    val df_user_socials = spark.table("temp_user_socials")

    df_users.show()
    df_user_socials.show()

    df_users.write.mode(SaveMode.Overwrite).format("csv").save("/demo/users.csv")
    df_users.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("market.users")
    df_user_socials.write.mode(SaveMode.Overwrite).format("csv").save("/demo/user_socials.csv")
    df_user_socials.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("market.user_socials")

    spark.stop()
  }
}
