package com.example.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Setup SparkSession
  * @author alanqtruong
  */
object SparkConfig {

  val conf: Config = ConfigFactory.load

  val sparkConf: SparkConf = new SparkConf(true)
      .setAppName(conf.getString("config.appName"))
      .setMaster(conf.getString("config.master"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  def getSparkSession: SparkSession = {
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate
    sparkSession.sparkContext.setLogLevel(conf.getString("config.logLevel"))
    sparkSession
  }
}