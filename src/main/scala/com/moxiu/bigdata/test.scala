package com.moxiu.bigdata

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("NewSimUser").setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlContext.jsonFile("/sources/apps/549c6462ba4d9b4d098b4567/exc/crash/19/01/1[4-5]")
      .filter("ctime>1547464800")
      .filter("ctime<1547522400")
      .select("base.imei", "base.androidid")
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(_._1)
      .count()


    sc.stop()

  }
}
