package com.moxiu.bigdata

import java.io.{IOException, _}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

object ProfileRankGenerator {
  val prop = new Properties()
  val inputStream = ProfileRankGenerator.getClass.getClassLoader.getResourceAsStream("settings.properties")
  //val inputStream = ProfileRankGenerator.getClass.getClassLoader.getResourceAsStream("s.properties")
  prop.load(inputStream)
  //用户设置
  System.setProperty("user.name", prop.getProperty("HADOOP_USER_NAME"))
  System.setProperty("HADOOP_USER_NAME", prop.getProperty("HADOOP_USER_NAME"))
  //更新相似标签
  val themeInfoPath = prop.getProperty("themeInfoPath")
  val maxNumOfSimilarTag = prop.getProperty("maxNumOfSimilarTag").toInt
  //更新用户画像
  val daysOfUserProfile = prop.getProperty("daysOfUserProfile").toInt
  val scanFactor = prop.getProperty("scanFactor").toFloat
  val downloadFactor = prop.getProperty("downloadFactor").toFloat
  val timeDecayFactor = prop.getProperty("timeDecayFactor").toFloat
  val contingencyFactor = prop.getProperty("contingencyFactor").toFloat
  val maxNumOfProfileTag = prop.getProperty("maxNumOfProfileTag").toInt
  //更新主题倒排表
  val maxNumOfThemePerTag = prop.getProperty("maxNumOfThemePerTag").toInt
  val daysForCalculatingHotTheme = prop.getProperty("daysForCalculatingHotTheme").toInt
  val numFilesPerRegion = prop.getProperty("numFilesPerRegion").toInt
  val recallFolder = prop.getProperty("recallFolder")
  //生成结果列表相关参数
  val numOfPartitionForGenerateResult = prop.getProperty("numOfPartitionForGenerateResult").toInt
  val minNumOfRecallTheme = prop.getProperty("minNumOfRecallTheme").toInt
  val minNumOfProfileWeight = prop.getProperty("minNumOfProfileWeight").toFloat
  val numOfThemePerUser = prop.getProperty("numOfThemePerUser").toInt
  val profileListOutputPath = prop.getProperty("profileListOutputPath")
  val downloadTable = prop.getProperty("downloadTable")
  //Hbase连接配置
  val hbaseConfiguration = HBaseConfiguration.create
  hbaseConfiguration.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"))
  hbaseConfiguration.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"))
  hbaseConfiguration.set("hbase.client.retries.number", prop.getProperty("hbase.client.retries.number"))
  hbaseConfiguration.set("hbase.client.pause", prop.getProperty("hbase.client.pause"))
  hbaseConfiguration.set("hbase.rpc.timeout", prop.getProperty("hbase.rpc.timeout"))
  hbaseConfiguration.set("hbase.client.operation.timeout", prop.getProperty("hbase.client.operation.timeout"))
  hbaseConfiguration.set("hbase.client.scanner.timeout.period", prop.getProperty("hbase.client.scanner.timeout.period"))
  val dfs_socket_timeout = prop.getProperty("dfs.socket.timeout")
  val dfs_client_socket_timeout = prop.getProperty("dfs.client.socket-timeout")
  val dfs_datanode_handler_count = prop.getProperty("dfs.datanode.handler.count")
  hbaseConfiguration.set("dfs.socket.timeout", dfs_socket_timeout)
  hbaseConfiguration.set("dfs.client.socket-timeout", dfs_client_socket_timeout);
  hbaseConfiguration.set("dfs.datanode.handler.count", dfs_datanode_handler_count);
  val connector = ConnectionFactory.createConnection(hbaseConfiguration)

  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("NewProfile")
      // .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("dfs.socket.timeout", dfs_socket_timeout)
    sc.hadoopConfiguration.set("dfs.client.socket-timeout", dfs_client_socket_timeout)
    sc.hadoopConfiguration.set("dfs.datanode.handler.count", dfs_datanode_handler_count)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val hadoopConf = new Configuration()
    hadoopConf.addResource("core-site.xml")
    hadoopConf.addResource("hdfs-site.xml")
    hadoopConf.addResource("yarn-site.xml")
    val shell = new FsShell(hadoopConf)
    val htable = new HTable(hbaseConfiguration, "ar:tag_theme")

    // val date = "2019-01-23 00:00:00"
    // val option = Array("makeRecommendList")
    val date = args(0)
    val option = args(1).split(",")
    //配置时间范围
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val when = df.parse(date)
    val before = df.format(new Date(when.getTime - daysOfUserProfile.toLong * 24 * 3600 * 1000L))
    val startDateForHot = df.format(new Date(when.getTime - daysForCalculatingHotTheme.toLong * 24 * 3600 * 1000L))

    /**
      * ==============================================  Hive配置  ==========================================================
      **/
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.exec.max.dynamic.partitions.pernode", "1000")
    sqlContext.sql("use theme_feed_sys")
    /**
      * ==============================================  清理路径  ==========================================================
      **/
    val hdfs = FileSystem.get(new java.net.URI("hdfs://nameservice1"), new Configuration())
    if (hdfs.exists(new Path(recallFolder))) {
      println("delete exits recallFolder tmp files") //清空临时文件目录
      hdfs.delete(new Path(recallFolder), true)
    }
    if (hdfs.exists(new Path(profileListOutputPath))) {
      println("delete exits profileList tmp files") //清空临时文件目录
      hdfs.delete(new Path(profileListOutputPath), true)
    }

    /**
      * =============================================  主题数据加载  =======================================================
      **/
    val themeInfo = sc.textFile(themeInfoPath)
      .repartition(60)
      .map(_.split("\t"))
      .map(line => (line(0), line(3), line(4), line(5), line(6), line(7))) //id,uid,grade,quality,ctime,tag
    val themeTag = themeInfo.map(info => (info._1, info._2, info._6.split(",|，").filter(!_.matches("\\s+|其他|[\\\\,|.|，|。|]+"))))
      .filter(_._3.size > 1)
      .map(line => (line._2, (line._1, line._3)))
      .aggregateByKey(mutable.Set[(String, Array[String])]())((set, one) => set += one, (set1, set2) => set1 ++= set2)
      .flatMap(authorLine => authorLine._2.flatMap(themeTuple => themeTuple._2.map(tag => (tag, (themeTuple._1, (authorLine._1, authorLine._2.size))))))
      .aggregateByKey(mutable.Set[(String, (String, Int))]())((set, one) => set += one, (set1, set2) => set1 ++= set2)
      .flatMap(tagLine => tagLine._2.map(themeLine => (themeLine, (tagLine._1, tagLine._2.size))))
      .aggregateByKey(mutable.Set[(String, Int)]())((set, one) => set += one, (set1, set2) => set1 ++= set2)

    /**
      * =============================================  更新用户画像  =======================================================
      **/
    if (option.contains("updateProfile")) {
      val input1 = sqlContext.sql("select time,theme_id,user_id,act_type " +
        "from theme_feed_sys.userbehavior where  pt>'" + before + "'").rdd
        .map(row => (row.getString(1), (row.getString(2), row.getString(3), row.getString(0))))
        .aggregateByKey(mutable.Set[(String, String, String)]())((set, one) => set += one, (set1, set2) => set1 ++= set2)
      val input2 = themeTag.map(themeLine => (themeLine._1._1, themeLine._2))
      val profile = input1.join(input2)
        .flatMap(line => line._2._1.map(record => (record._1, (record._2, record._3, line._2._2))))
        .aggregateByKey(mutable.Set[(String, String, mutable.Set[(String, Int)])]())((set, one) => set += one, (set1, set2) => set1 ++= set2)
        .map { userLine =>
          val output = userLine._2.toList.flatMap { theme =>
            val actFactor = theme._1 match {
              case "Scan" => scanFactor
              case "Download" => downloadFactor
              case _ => 0.0F
            }
            val timestamp = try {
              df.parse(theme._2).getTime
            } catch {
              case e: Exception => new Date().getTime - 1000 * 3600 * 24 * daysOfUserProfile
            }
            val timeContext = (new Date().getTime - timestamp) / 1000 / 3600 / 24
            val timeFactor = 1 / (1 + timeDecayFactor * timeContext.toDouble)
            val res = theme._3.map { tag =>
              val contingency = math.log10(tag._2 / userLine._2.size)
              val costForContingency = if (contingency > 0) contingency else 0.0
              val heatFactor = 1 / (1 + contingencyFactor * costForContingency)
              (tag._1, actFactor * timeFactor * heatFactor)
            }
            res
          }.groupBy(_._1).mapValues(x => x.map(_._2).sum.toFloat).toList.sortBy(_._2).reverse.take(maxNumOfProfileTag)
          (userLine._1, normalization(output).toMap)
        }
      val rowProfile = profile.map { x => Row(x._1, x._2) }
      val schemaForProfile = StructType(
        List(
          StructField("user_id", StringType, false),
          StructField("profile_tag", MapType(StringType, FloatType, false), false)
        )
      )
      sqlContext.createDataFrame(rowProfile, schemaForProfile).insertInto("profile_result", true)
    }

    /**
      * ============================================  更新相似标签及召回主题库  ============================================
      **/
    if (option.contains("updateSimTagAndRecallTheme")) {
      val simTag = themeTag
        .filter(_._2.size > 1)
        .flatMap(line => line._2.toList.combinations(2).map(two => (two, line._1._2)))
        .flatMap { twoTag =>
          val tag1 = twoTag._1.head
          val tag2 = twoTag._1.last
          val author = twoTag._2
          val factor = 1 / (1 + math.log10(author._2)) / math.sqrt(tag1._2.toDouble * tag2._2.toDouble)
          List(((tag1._1, tag2._1), factor), ((tag2._1, tag1._1), factor))
        }
        .reduceByKey(_ + _)
        .map { x => (x._1._1, (x._1._2, x._2.toFloat)) }
        .aggregateByKey(mutable.Set[(String, Float)]())((set, one) => set += one, (set1, set2) => set1 ++= set2)
        .map(tagLine => (tagLine._1, normalization(tagLine._2.toList.sortBy(_._2).reverse.take(maxNumOfSimilarTag))))
      val recallTheme = themeInfo
        .map(line => (line._1, line._3, line._4, line._5, line._6.split(",|，").filter(!_.matches("\\s+|其他|[\\\\,|.|，|。|]+")))) //id,grade,quality,ctime,tag
        .flatMap { themeLine => themeLine._5.map(tag => (tag, (themeLine._1, themeLine._2, themeLine._3, themeLine._4, themeLine._5))) }
        .aggregateByKey(mutable.Set[(String, String, String, String, Array[String])]())((set, one) => set += one, (set1, set2) => set1 ++= set2)
        .map { tagLine =>
          val sortedList = tagLine._2
            .toList
            .sortBy(theme => (theme._2, theme._3, theme._4))(Ordering.Tuple3(Ordering.String.reverse, Ordering.String.reverse, Ordering.String.reverse))
          val limitedList = if (sortedList.length > maxNumOfThemePerTag) sortedList.take(maxNumOfThemePerTag) else sortedList
          (tagLine._1, limitedList)
        }
      val dataToLoadHbase = simTag.join(recallTheme, new HFilePartitioner(hbaseConfiguration, htable.getStartKeys, numFilesPerRegion))
        .mapPartitions { partition =>
          import scala.collection.JavaConverters._
          val kvs = new util.TreeSet[KeyValue](KeyValue.RAW_COMPARATOR)
          partition
            .flatMap { one =>
              List(new KeyValue(Bytes.toBytes(one._1), Bytes.toBytes("a"), Bytes.toBytes("ls"), objToByteArray(one._2._2)),
                new KeyValue(Bytes.toBytes(one._1), Bytes.toBytes("a"), Bytes.toBytes("tg"), objToByteArray(one._2._1)))
            }
            .foreach(one => kvs.add(one))
          kvs.iterator().asScala.map(one => (new ImmutableBytesWritable(one.getKey), one))
        }
        .saveAsNewAPIHadoopFile(recallFolder,
          classOf[ImmutableBytesWritable],
          classOf[KeyValue],
          classOf[HFileOutputFormat2],
          hbaseConfiguration)
      try
        shell.run(Array[String]("-chmod", "-R", "777", recallFolder))
      catch {
        case e: Exception =>
          println("Couldnt change the file permissions ", e)
          throw new IOException(e)
      }
      val load = new LoadIncrementalHFiles(hbaseConfiguration)
      val table: Table = connector.getTable(TableName.valueOf("ar:tag_theme"))
      try {
        //获取hbase表的region分布
        // val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
        val job = Job.getInstance(hbaseConfiguration)
        job.setJobName("DumpFile")
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])
        HFileOutputFormat2.configureIncrementalLoadMap(job, table)
        //开始导入
        load.doBulkLoad(new Path(recallFolder), table.asInstanceOf[HTable])
      } finally {
        table.close()
      }
    }

    /**
      * ============================================  推荐列表生成  ========================================================
      **/
    if (option.contains("makeRecommendList")) {
      val mixedProfile = sqlContext.sql("select * from profile_result").rdd
        .map(row => (row.getString(0), row.getMap[String, Float](1)))
        .flatMap(line => line._2.map(tag => (tag._1, (line._1, tag._2))))
        .aggregateByKey(mutable.Set[(String, Float)](), numOfPartitionForGenerateResult)((set, one) => set += one, (set1, set2) => set1 ++= set2)
        .filter(x => x._1 != null && x._1.nonEmpty && x._1.length > 0)
        .mapPartitions { partition =>
          val table = connector.getTable(TableName.valueOf("ar:tag_theme"))
          val res = partition.flatMap { line =>
            val themeRes = table
              .get(new Get(Bytes.toBytes(line._1)).addColumn(Bytes.toBytes("a"), Bytes.toBytes("ls")))
              .value()
            val themeList = if (themeRes != null) byteArrayToObj(themeRes).asInstanceOf[List[(String, String, String, String, Array[String])]] else List[(String, String, String, String, Array[String])]()
            val tagRes = table
              .get(new Get(Bytes.toBytes(line._1)).addColumn(Bytes.toBytes("a"), Bytes.toBytes("tg")))
              .value()
            val tagList = if (tagRes != null) byteArrayToObj(tagRes).asInstanceOf[List[(String, Float)]] else List[(String, Float)]()
            val output = line._2.map { user =>
              (user._1, (line._1, user._2, themeList, tagList))
            }
            output
          }
          table.close()
          res
        }
        .aggregateByKey(mutable.Set[(String, Float, List[(String, String, String, String, Array[String])], List[(String, Float)])]())((set, one) => set += one, (set1, set2) => (set1 ++= set2).filter(tag => tag._3.size > 0 && tag._4.size > 0))
        .filter(x => x._1 != null && x._1.nonEmpty && x._1.length > 0 && x._2.size > 0)
        .map { user =>
          val theme = user._2
            .flatMap(_._3)
          val originProfile = user._2
            .map(record => (record._1, record._2))
            .toMap
          val extendProfile = user._2
            .toList
            .flatMap(record => record._4.map(tag => (tag._1, tag._2 * record._2)))
            .groupBy(_._1)
            .mapValues(_.map(_._2).sum)
          (user._1, originProfile, extendProfile, theme)
        }
        .mapPartitions { partition =>
          val tableInPartition = connector.getTable(TableName.valueOf("ar:tag_theme"))
          val tableForDownload: Table = connector.getTable(TableName.valueOf(downloadTable))
          val res = partition.map { record =>
            var inputProfile = record._3
            val myHistory = (getThemeSet(tableForDownload, record._1, "good") ++ getThemeSet(tableForDownload, record._1, "bad")).map(_._1)
            val outPutSet = record._4.filterNot(theme => myHistory.contains(theme._1))
            while (outPutSet.size < minNumOfRecallTheme && inputProfile.values.filter(_ < minNumOfProfileWeight).size < inputProfile.values.size) {
              val thisTermResult = getTheme(tableInPartition, inputProfile)
              outPutSet ++= thisTermResult._1.filterNot(theme => myHistory.contains(theme._1))
              inputProfile = thisTermResult._2
            }
            (record._1, record._2, outPutSet)
          }
          tableInPartition.close()
          tableForDownload.close()
          res
        }
        .map { record =>
          val list = record._3
            .map(theme => (theme._1, theme._2, theme._3, theme._4, record._2.filterKeys(theme._5.contains(_)).values.sum))
            .toList // score,quality,ctime
            .sortBy(one => (one._5, one._3, one._4))(Ordering.Tuple3(Ordering[Float].reverse, Ordering[String].reverse, Ordering[String].reverse))
          val profileList = list.take(numOfThemePerUser).map(_._1)
          val extendList = list.reverse.take(numOfThemePerUser).map(_._1)
          record._1 + "\t" + profileList.mkString("\002") + "\t" + extendList.mkString("\002")
        }
        //         (record._1,profileList)
        //     }.foreach(x=>println(x._1,x._2.take(3).mkString(",")))
        .saveAsTextFile(profileListOutputPath)
      try
        shell.run(Array[String]("-chmod", "-R", "777", profileListOutputPath))
      catch {
        case e: Exception =>
          println("Couldnt change the file permissions ", e)
          throw new IOException(e)
      }
    }

    connector.close()
    sc.stop()
  }

  /**
    * 从Hbase中获取用户的主题下载记录
    */
  def getThemeSet(table: Table, rowkey: String, qualifier: String) = {
    val get = new Get(Bytes.toBytes(rowkey))
    get.addColumn(Bytes.toBytes("a"), Bytes.toBytes(qualifier))
    var resSet = mutable.Set[(String, String, String, String)]()
    if (table.exists(get)) {
      val hasSet = table.get(get).getValue(Bytes.toBytes("a"), Bytes.toBytes(qualifier))
      resSet = byteArrayToObj(hasSet).asInstanceOf[mutable.Set[(String, String, String, String)]]
    }
    resSet
  }

  /**
    * 从Hbase中召回主题并扩展画像
    */
  def getTheme(table: Table, inputProfile: scala.collection.Map[String, Float]) = {
    import scala.collection.JavaConversions._
    val tagAndWeight = inputProfile.toList.filter(tag => tag._1 != null && tag._1.size > 0)
    val getThemeList = for (tag <- tagAndWeight) yield new Get(Bytes.toBytes(tag._1)).addColumn(Bytes.toBytes("a"), Bytes.toBytes("ls"))
    val theme = table
      .get(getThemeList)
      .flatMap { result =>
        if (result.containsNonEmptyColumn(Bytes.toBytes("a"), Bytes.toBytes("ls"))) {
          byteArrayToObj(result.value()).asInstanceOf[List[(String, String, String, String, Array[String])]]
        } else {
          List[(String, String, String, String, Array[String])]()
        }
      }
      .toSet
    val getTagList = for (tag <- tagAndWeight) yield new Get(Bytes.toBytes(tag._1)).addColumn(Bytes.toBytes("a"), Bytes.toBytes("tg"))
    val similarTag = table
      .get(getTagList)
      .map { result =>
        if (result.containsNonEmptyColumn(Bytes.toBytes("a"), Bytes.toBytes("tg"))) {
          (Bytes.toString(result.getRow), byteArrayToObj(result.value()).asInstanceOf[List[(String, Float)]])
        } else {
          (Bytes.toString(result.getRow), List[(String, Float)]())
        }
      }
      .toMap
    val extendProfile = (for (tag <- tagAndWeight) yield {
      similarTag.get(tag._1) match {
        case a: Some[List[(String, Float)]] => a.get.map(one => (one._1, one._2 * tag._2))
        case _ => List[(String, Float)]()
      }
    })
      .flatten
      .groupBy(_._1)
      .mapValues(value => value.map(_._2).sum)
      .filterKeys(!inputProfile.keySet.contains(_))
    (theme, extendProfile)
  }

  /**
    * ByteArray转成object
    */
  def byteArrayToObj(bytes: Array[Byte]) = {
    var obj: Any = null
    try {
      val bis = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bis)
      obj = ois.readObject()
      ois.close
      bis.close
    } catch {
      case e: Exception => e.printStackTrace
    }
    obj
  }

  /**
    * 将权重归一化
    **/
  def normalization(l: List[(String, Float)]) = {
    var list = l.sortBy(_._2).reverse
    val big = list.head._2
    val small = list.last._2
    list match {
      case ls if ls.length == 1 => list = List((ls.head._1, 1.0F))
      case ls if ls.length == 2 => list = List((ls.head._1, 1.0F), (ls.last._1, 0.1F))
      case ls if ls.length > 2 => list = ls.map(tag => (tag._1, if (big == small) 1.0F else (0.1F + 0.9F * (tag._2 - small) / (big - small))))
      case _ =>
    }
    list
  }

  /**
    * object 转成ByteArray
    */
  def objToByteArray(obj: Any) = {
    var bytes: Array[Byte] = null
    val bos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bos)
    try {
      oos.writeObject(obj)
      oos.flush
      bytes = bos.toByteArray
    } catch {
      case ex: IOException => ex.printStackTrace();
    } finally {
      oos.close
      bos.close
    }
    bytes
  }

  /**
    * 从规定范围内生成随机数List
    */
  def randomList(min: Int, max: Int, n: Int) = {
    var arr = min to max toArray
    var outList: List[Int] = Nil
    var border = arr.length //随机数范围
    for (i <- 0 to n - 1) {
      //生成n个数
      val index = (new Random).nextInt(border)
      outList = outList ::: List(arr(index))
      arr(index) = arr.last //将最后一个元素换到刚取走的位置
      arr = arr.dropRight(1) //去除最后一个元素
      border -= 1
    }
    outList
  }


}
