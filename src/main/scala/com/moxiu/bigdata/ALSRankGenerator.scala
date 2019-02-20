package com.moxiu.bigdata

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object ALSRankGenerator {

  def main(args: Array[String]): Unit = {
    /**
      * ==============================================  参数设置  ==========================================================
      **/
    if (args.length < 5 || args.length > 10) {
      println("Usage: ALS <master> <ratings_file> <rank> <iterations> <output_dir> " +
        "[<lambda>] [<implicitPrefs>] [<alpha>] [<blocks>]")
      System.exit(1)
    }
    val (master, ratingsFile, rank, iters, outputDir) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4))
    val lambda = if (args.length >= 6) args(5).toDouble else 0.01
    val implicitPrefs = if (args.length >= 7) args(6).toBoolean else false
    val alpha = if (args.length >= 8) args(7).toDouble else 1
    val blocks = if (args.length >= 9) args(8).toInt else -1
    val sep = if (args.length == 9) args(9) else "::"
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
      .set("spark.kryo.referenceTracking", "false")
      .set("spark.kryoserializer.buffer.mb", "512")
      .setAppName("ALS")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val before = "2019-02-10"

    /**
      * ==============================================  Hive配置  ==========================================================
      **/
    sqlContext.sql("use theme_feed_sys")
    /**
      * ==============================================  清理路径  ==========================================================
      **/
    val hdfs = FileSystem.get(new java.net.URI("hdfs://nameservice1"), new Configuration())
    if (hdfs.exists(new Path(ratingsFile))) {
      println("delete exits recallFolder tmp files") //清空临时文件目录
      hdfs.delete(new Path(ratingsFile), true)
    }
    /**
      * ==============================================  数据预处理  ========================================================
      **/
    val input = sqlContext.sql("select time,theme_id,user_id,act_type " +
      "from theme_feed_sys.userbehavior where  pt>'" + before + "'").rdd.persist(StorageLevel.DISK_ONLY)
    val userIndex = input.map(_.getString(2)).aggregate(mutable.Set[String]())((set, s) => set += s, (set1, set2) => set1 ++= set2).toArray.zipWithIndex.toMap
    val themeIndex = input.map(_.getString(1)).aggregate(mutable.Set[String]())((set, s) => set += s, (set1, set2) => set1 ++= set2).toArray.zipWithIndex.toMap
    val data = input.map { row =>
      val actFactor = row.getString(3) match {
        case "Scan" => 1D
        case "Download" => 7D
        case _ => 0D
      }
      val timestamp = try {
        df.parse(row.getString(0)).getTime
      } catch {
        case e: Exception => new Date().getTime - 1000 * 3600 * 24 * 180
      }
      val timeContext = (new Date().getTime - timestamp) / 1000 / 3600 / 24
      val timeFactor = 1 / (1 + 0.1 * timeContext.toDouble)
      (userIndex(row.getString(2)) + sep + themeIndex(row.getString(1)) + sep + actFactor * timeFactor)
    }
      .saveAsTextFile(ratingsFile)
    input.unpersist()
    /**
      * ==============================================  训练模型  ==========================================================
      **/
    val ratings = sc.textFile(ratingsFile).map { line =>
      val fields = line.split(sep)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    /*
    val model = new ALS(rank = rank, iterations = iters, lambda = lambda,
      numBlocks = blocks, implicitPrefs = implicitPrefs, alpha = alpha).run(ratings)
    */
    val model = ALS.trainImplicit(ratings, rank, iters, lambda, blocks, alpha)
    /*
    if (implicitPrefs) {
      val model = ALS.trainImplicit(ratings, rank, iters, lambda, blocks, alpha)
    }
    else {
      val model = ALS.train(ratings, rank, iters, lambda, blocks)
    }
    */
    model.userFeatures.map { case (id, vec) => id + "," + vec.mkString(" ") }
      .saveAsTextFile(outputDir + "/userFeatures")
    model.productFeatures.map { case (id, vec) => id + "," + vec.mkString(" ") }
      .saveAsTextFile(outputDir + "/productFeatures")
    println("Final user/product features written to " + outputDir)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product) }
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rate) => user.toString + sep + product.toString + sep + rate.toString
    }
    predictions.saveAsTextFile(outputDir + "/predictions")
    /*
    val ratesAndPreds = ratings.map{
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map{
      case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
    }.reduce(_ + _)/ratesAndPreds.count
    println("Mean Squared Error = " + MSE)
    */

    sc.stop()

  }

  private class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }

}
