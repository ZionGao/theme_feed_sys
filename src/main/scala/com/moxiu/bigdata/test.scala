package com.moxiu.bigdata

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object test extends App {

  val text = "1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy"

  println(text.split("::", 2).mkString(","))





}
