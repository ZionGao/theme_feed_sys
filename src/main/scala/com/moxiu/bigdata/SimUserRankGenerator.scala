package com.moxiu.bigdata

import java.io._

import org.apache.hadoop.fs.FsShell
import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission

import scala.collection.mutable
import scala.util.Random

object SimUserRankGenerator {
  val prop = new Properties()
  val inputStream = SimUserRankGenerator.getClass.getClassLoader.getResourceAsStream("settings.properties")
  //val inputStream = SimUserRankGenerator.getClass.getClassLoader.getResourceAsStream("s.properties")
  prop.load(inputStream)
  //用户设置
  System.setProperty("user.name", prop.getProperty("HADOOP_USER_NAME"))
  System.setProperty("HADOOP_USER_NAME", prop.getProperty("HADOOP_USER_NAME"))
  //更新主题信息相关参数
  val themeInfoPath = prop.getProperty("themeInfoPath")
  val stagingFolder = prop.getProperty("stagingFolder")
  val themeInfoTable = prop.getProperty("themeInfoTable")
  val numFilesPerRegionForThemeInfo = prop.getProperty("numFilesPerRegionForThemeInfo").toInt
  //更新用户下载历史相关参数
  val daysForUpdatingDownloadHistory = prop.getProperty("daysForUpdatingDownloadHistory").toInt
  val numOfPartitionPerDayForDownloadHistory = prop.getProperty("numOfPartitionPerDayForDownloadHistory").toInt
  val downloadTable = prop.getProperty("downloadTable")
  //生成备用主题相关参数
  val daysForCalculatingHotTheme = prop.getProperty("daysForCalculatingHotTheme").toInt
  val numOfHotTheme = prop.getProperty("numOfHotTheme").toInt
  //协同过滤计算相关参数
  val numOfPartitionPerDay = prop.getProperty("numOfPartitionPerDay").toInt
  val numOfSimUserPerUser = prop.getProperty("numOfSimUserPerUser").toInt
  val daysOfUserBehaviorForCF = prop.getProperty("daysOfUserBehaviorForCF").toInt
  val scanFactor = prop.getProperty("scanFactor").toFloat
  val downloadFactor = prop.getProperty("downloadFactor").toFloat
  val themeMaxUser = prop.getProperty("themeMaxUser").toInt
  //生成结果列表相关参数
  val numOfPartitionFOrReadingUserTheme = prop.getProperty("numOfPartitionFOrReadingUserTheme").toInt
  val maxNumOfThemeFromSimUser = prop.getProperty("maxNumOfThemeFromSimUser").toInt
  val listSizeOfSimRecommend = prop.getProperty("listSizeOfSimRecommend").toInt
  val simUserListOutputPath = prop.getProperty("simUserListOutputPath")
  //Hbase连接配置
  val hbaseConfiguration = HBaseConfiguration.create
  hbaseConfiguration.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"))
  hbaseConfiguration.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"))
  hbaseConfiguration.set("hbase.client.retries.number", prop.getProperty("hbase.client.retries.number"))
  hbaseConfiguration.set("hbase.client.pause", prop.getProperty("hbase.client.pause"))
  hbaseConfiguration.set("hbase.rpc.timeout", prop.getProperty("hbase.rpc.timeout"))
  hbaseConfiguration.set("hbase.client.operation.timeout", prop.getProperty("hbase.client.operation.timeout"))
  hbaseConfiguration.set("hbase.client.scanner.timeout.period", prop.getProperty("hbase.client.scanner.timeout.period"))
  //Hbase连接配置
  val dfs_socket_timeout = prop.getProperty("dfs.socket.timeout")
  val dfs_client_socket_timeout = prop.getProperty("dfs.client.socket-timeout")
  val dfs_datanode_handler_count = prop.getProperty("dfs.datanode.handler.count")
  hbaseConfiguration.set("dfs.socket.timeout", dfs_socket_timeout)
  hbaseConfiguration.set("dfs.client.socket-timeout", dfs_client_socket_timeout);
  hbaseConfiguration.set("dfs.datanode.handler.count", dfs_datanode_handler_count);

  val connector = ConnectionFactory.createConnection(hbaseConfiguration)

  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("NewSimUser")
      //  .setMaster("local[2]")
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
    val date = args(0)
    val option = args(1).split(",")
    //配置时间范围
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val when = df.parse(date)
    val before = df.format(new Date(when.getTime - daysOfUserBehaviorForCF.toLong * 24 * 3600 * 1000L))
    val startDate = df.format(new Date(when.getTime - daysForUpdatingDownloadHistory.toLong * 24 * 3600 * 1000L))
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
    if (hdfs.exists(new Path(stagingFolder))) {
      println("delete exits themeInfo tmp files") //清空临时文件目录
      hdfs.delete(new Path(stagingFolder), true)
    }
    if (hdfs.exists(new Path(simUserListOutputPath))) {
      println("delete exits simUserList tmp files") //清空临时文件目录
      hdfs.delete(new Path(simUserListOutputPath), true)
    }

    /**
      * ========================================  更新主题信息Hbase  =======================================================
      **/
    if (option.contains("updateTheme")) //参数中包含updateTheme则进行更新主题信息操作
    {
      val htable = new HTable(hbaseConfiguration, "ar:recommend_themeinfo")
      val hfileRDD = sc.textFile(themeInfoPath)
        .map(line => (line.split("\t")(0), line))
        .partitionBy(new HFilePartitioner(hbaseConfiguration, htable.getStartKeys, numFilesPerRegionForThemeInfo))
        .mapPartitions { partition =>
          import scala.collection.JavaConverters._
          val kvs = new util.TreeSet[KeyValue](KeyValue.RAW_COMPARATOR)
          partition
            .flatMap { themeLine =>
              val themeInfo = themeLine._2.split("\t")
              val scheme = Array("id", "is_share", "status", "uid", "grade", "quality", "ctime", "tags")
              val rowKey = themeInfo(0)
              val kv = scheme.zip(themeInfo).drop(1)
              val list = for (i <- kv) yield new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("a"), Bytes.toBytes(i._1), Bytes.toBytes(i._2))
              list
            }
            .foreach(one => kvs.add(one))
          kvs.iterator().asScala.map(one => (new ImmutableBytesWritable(one.getKey), one))
        }
        .saveAsNewAPIHadoopFile(stagingFolder,
          classOf[ImmutableBytesWritable],
          classOf[KeyValue],
          classOf[HFileOutputFormat2],
          hbaseConfiguration)

      if (hdfs.exists(new Path(stagingFolder))) {
        println("chmod exits files") //向Hbase Load需要临时文件写权限
        val changedPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, true)
        val fileList = hdfs.listFiles(new Path(stagingFolder), true)
        while (fileList.hasNext()) {
          hdfs.setPermission(fileList.next().getPath(), changedPermission)
        }
      }

      val shell = new FsShell(hadoopConf)
      try
        shell.run(Array[String]("-chmod", "-R", "777", stagingFolder))
      catch {
        case e: Exception =>
          println("Couldnt change the file permissions ", e)
          throw new IOException(e)
      }

      val load = new LoadIncrementalHFiles(hbaseConfiguration)
      val table: Table = connector.getTable(TableName.valueOf("ar:recommend_themeinfo"))
      try {
        //获取hbase表的region分布
        // val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
        val job = Job.getInstance(hbaseConfiguration)
        job.setJobName("DumpFile")
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])
        HFileOutputFormat2.configureIncrementalLoadMap(job, table)
        //开始导入
        load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])
      } finally {
        table.close()
      }
    }

    /**
      * =================================  昨日用户下载与历史记录Merge入Hbase  =============================================
      **/
    if (option.contains("updateDownloadHistory")) { //参数中包含updateDownloadHistory则进行更新历史记录操作
      sqlContext.sql("select theme_id,user_id " +
        "from theme_feed_sys.userbehavior where  pt>='" + startDate + "' and act_type='Download'").rdd //读取Hive数据
        .map(row => (row.getString(0), row.getString(1)))
        .filter(x => x._1.nonEmpty && x._1.length > 0)
        .aggregateByKey(mutable.Set[String](), daysForUpdatingDownloadHistory * numOfPartitionPerDayForDownloadHistory)((set, user) => set += user, (set1, set2) => set1 ++= set2)
        .mapPartitions { partition => //从Hbase中匹配主题quality，grade，ctime字段之后应用于推荐规则
          val table: Table = connector.getTable(TableName.valueOf(themeInfoTable))
          val res = partition.map { record =>
            val getList = for (qualifier <- List("quality", "grade", "ctime"))
              yield new Get(Bytes.toBytes(record._1)).addColumn(Bytes.toBytes("a"), Bytes.toBytes(qualifier))
            import scala.collection.JavaConversions._
            val resList = for (result <- table.get(getList))
              yield result.rawCells().map(f => ((Bytes.toString(f.getQualifier), Bytes.toString(f.getValue))))
            val outPut = Array("0", "3", "1514796503") //默认较低值
            try {
              resList.flatten.foreach { one =>
                one._1 match {
                  case "quality" => outPut(0) = one._2
                  case "grade" => outPut(1) = one._2
                  case "ctime" => outPut(2) = one._2
                  case _ =>
                }
              }
            } catch {
              case e: Exception =>
            }
            ((record._1, outPut(0), outPut(1), outPut(2)), record._2)
          }
          table.close()
          res
        }
        .repartition(daysForUpdatingDownloadHistory * numOfPartitionPerDayForDownloadHistory)
        .flatMap(themeLine => themeLine._2.map(user => (user, themeLine._1)))
        .aggregateByKey(mutable.Set[(String, String, String, String)](), daysForUpdatingDownloadHistory * numOfPartitionPerDayForDownloadHistory)((set, theme) => set += theme, (set1, set2) => set1 ++= set2)
        .map { userLine =>
          val goodTheme = userLine._2.filter(_._3 == "4")
          val badTheme = userLine._2 -- goodTheme
          (userLine._1, goodTheme, badTheme)
        } //将用户下载过的优质主题与非优质主题分开存放，增量更新至Hbase
        .foreachPartition { partition =>
        val table: Table = connector.getTable(TableName.valueOf(downloadTable))
        partition.foreach { record =>
          mergeHbaseData(record._2, table, record._1, "a", "good")
          mergeHbaseData(record._3, table, record._1, "a", "bad")
        }
        table.close()
      }
    }

    /**
      * ============================================  备用主题生成  ========================================================
      **/
    if (option.contains("makeHotList")) { //参数中包含makeHotList则进行更新备用主题操作
      val reserveTheme = sqlContext.sql("select theme_id " +
        "from theme_feed_sys.userbehavior where  pt>='" + startDateForHot + "' and act_type='Download'").rdd
        .map(row => (row.getString(0), 1))
        .reduceByKey(_ + _)
        .mapPartitions { partition => //从Hive中读取历史若干天下载量较高的主题，并依据grade，quality，ctime排序
          val table: Table = connector.getTable(TableName.valueOf(themeInfoTable))
          val res = partition.map { theme =>
            val themeinfo = getThemefeature(table, theme._1, mutable.Buffer("grade", "quality", "ctime"), mutable.Buffer("3", "0", "1514796503"))
            (themeinfo(1) + List.fill(8 - theme._2.toString.size)(0).mkString + theme._2 + themeinfo(2), (theme._1, theme._2, themeinfo(0), themeinfo(1), themeinfo(2)))
          }
          table.close()
          res
        }
        .sortByKey(false, 1)
        .top(numOfHotTheme) //取1000个用以补充相似用户算法可能某些用户生成主题较少的情况
        .zipWithIndex
        .map(line => Row(line._2.toString, line._1._2._1, line._1._2._2, line._1._2._3, line._1._2._4, line._1._2._5))
      val schemaForThemeRank = StructType(
        List(
          StructField("rank", StringType, true),
          StructField("id", StringType, true),
          StructField("dn", StringType, true),
          StructField("grade", StringType, true),
          StructField("quality", StringType, true),
          StructField("ctime", StringType, true)
        )
      )
      val dataFrame = sqlContext.createDataFrame(sc.parallelize(reserveTheme, 1), schemaForThemeRank)
        .insertInto("reserve_theme_rank", true)
    }
    /**
      * ============================================  协同过滤计算  ========================================================
      **/
    val input = sqlContext.sql("select time,theme_id,user_id,act_type " +
      "from theme_feed_sys.userbehavior where  pt>'" + before + "'").rdd
    val computeRDD = input //从Hive中读取 time时间戳，theme_id主题id，user_id用户id，act_type行动类别
      .map(row => ((row.getString(2), row.getString(3)), row.getString(1)))
      .aggregateByKey(collection.mutable.Set[String](), daysOfUserBehaviorForCF * 3)(
        (set, one) => set += one,
        (set1, set2) => set1 ++= set2) // 统计某一用户 在窗口时间内 浏览或者下载 主题的个数
      .flatMap(userline => for (one <- userline._2) yield ((one, userline._1._2), (userline._1._1, userline._2.size)))
      .aggregateByKey(collection.mutable.Set[(String, Int)](), daysOfUserBehaviorForCF * 3)(
        (set, one) => set += one,
        (set1, set2) => set1 ++= set2) // 统计某一主题 在窗口时间内 被浏览或者下载 的次数
      .map { themeline => ((themeline._1._1, themeline._1._2, themeline._2.size), themeline._2.toList) }
      .filter(_._1._3 > 1)
      .map { themeline =>
        val userList = if (themeline._2.size < themeMaxUser) themeline._2 else
          for (i <- randomList(0, themeline._2.size - 1, themeMaxUser)) yield themeline._2(i)
        (themeline._1, userList)
      } // 某一主题 如果浏览下载的用户过多，从其中随机选取若干位，否则计算代价过大
      .repartition(numOfPartitionPerDay * daysOfUserBehaviorForCF)
      .flatMap(List => for (twoUser <- List._2.combinations(2)) yield (twoUser.head, twoUser.last, List._1))
      .flatMap { element => // 从某一主题浏览下载的用户中，随机选取两两组合，作为一对用户的共现相似因子
        val actFactor = element._3._2 match { // 计算该次两个用户在同一个主题中共同出现，贡献多少相似因子
          case "Scan" => scanFactor
          case "Download" => downloadFactor
          case _ => 0.0F
        } //相似因子受行动类别影响
      val heatFactor = 1 / math.log(1 + element._3._3.toFloat) //相似因子受主题的热度影响
      val simVector = 1 / math.sqrt(element._1._2.toFloat * element._2._2.toFloat) //以余弦向量计算相似度
        List(((element._1._1, element._2._1), (actFactor * heatFactor * simVector).toFloat),
          ((element._2._1, element._1._1), (actFactor * heatFactor * simVector).toFloat))
      }
      .aggregateByKey(0.0F)(_ + _, _ + _) //聚合累加所有相同用户两两组合的相似因子
      .map(element => (element._1._1, (element._1._2, element._2)))
      .aggregateByKey(mutable.Buffer[(String, Float)](), numOfPartitionPerDay * daysOfUserBehaviorForCF)(
        (buffer, elem) => buffer += elem, //聚合累加某用户所有的相似用户及对应的相似因子
        (buffer1, buffer2) => buffer1 ++= buffer2) //排序得到相似度依次下降的相似用户队列结果
      .map(simList => (simList._1, simList._2.sortBy(_._2).reverse.take(numOfSimUserPerUser).toMap)).persist()

    /**
      * =======================================  相似用户数据入Hive  =======================================================
      **/
    if (option.contains("loadSimUserDataIntoHive")) { //参数中包含loadSimUserDataIntoHive则将相似用户结果更新至Hive
      val row = computeRDD
        .map { x => Row(x._1, x._2) }

      val schema = StructType(
        List(
          StructField("user_id", StringType, false),
          StructField("similar_user", MapType(StringType, FloatType, false), false)
        )
      )
      sqlContext.createDataFrame(row, schema).insertInto("similar_user_result", true)
    }

    /**
      * ============================================  推荐列表生成  ========================================================
      **/
    if (option.contains("makeRecommendList")) { //参数中包含makeRecommendList则制作推荐主题列表
      val resTheme = sqlContext.sql("select rank,id from reserve_theme_rank").rdd
        .map(row => (row.getString(0), row.getString(1)))
        .collect()
        .sortBy(_._1)
        .map(_._2)
      val broadTheme = sc.broadcast(resTheme)

      val listRDD = if (!option.contains("existSimUserData")) {
        computeRDD
      } else { //选择依据本次计算的相似用户数据或者从Hive中读取已有的相似用户数据
        sqlContext.sql("select * from similar_user_result").rdd
          .map(row => (row.getString(0), row.getMap[String, Float](1)))
      }
        .map(one => (one._1, one._2.toList.sortBy(_._2).reverse.map(_._1).filter(_.nonEmpty)))
        .filter(_._1.nonEmpty)
        .repartition(numOfPartitionFOrReadingUserTheme)
        .mapPartitions { partition => //根据用户的相似用户列表，从Hbase读取其他用户历史下载主题推荐给该用户
          val table: Table = connector.getTable(TableName.valueOf(downloadTable))
          val res = partition.map { record =>
            val myHistory = getThemeSet(table, record._1, "good") ++ getThemeSet(table, record._1, "bad") // 本人下载历史
          var howManyIGet = 0 // 目前获得多少个推荐主题
          val simUserIteroter = record._2.iterator
            val recommendTheme = mutable.Set[(String, String, String, String)]()

            while (howManyIGet < maxNumOfThemeFromSimUser && simUserIteroter.hasNext) { // 推荐主题数量不满足要求且仍有相似用户则循环取主题
              val otherHistory = getThemeSet(table, simUserIteroter.next(), "good")
              recommendTheme ++= otherHistory
              howManyIGet = recommendTheme.size
            }
            val sortedList = (recommendTheme -- myHistory)
              .toBuffer.sortBy[(String, String)](x => (x._2, x._4))(Ordering.Tuple2[String, String]) // 推荐主题依据ctime 及 quality排序
              .reverse
            (record._1, sortedList)
          }
          table.close()
          res
        }
        .map { themeList =>
          val rowList = themeList._2
          val lack = listSizeOfSimRecommend - rowList.size // 判断推荐主题是否仍不满足数量要求，少补多退
        val res =
          if (lack < 0) {
            rowList.dropRight(-lack).map(_._1)
          } else if (lack > 0) {
            val goodTheme = themeList._2.filter(_._2 == "1").map(_._1)
            val notBadTheme = themeList._2.map(_._1) -- goodTheme
            goodTheme ++ broadTheme.value.take(lack).toBuffer ++ notBadTheme
          } else {
            rowList.map(_._1)
          }
          (themeList._1 + "\t" + res.mkString("\002"))
        }
        .saveAsTextFile(simUserListOutputPath) // GG
    }
    val shell = new FsShell(hadoopConf)
    try
      shell.run(Array[String]("-chmod", "-R", "777", stagingFolder))
    catch {
      case e: Exception =>
        println("Couldnt change the file permissions ", e)
        throw new IOException(e)
    }

    computeRDD.unpersist()
    connector.close()
    sc.stop()
  }

  /**
    * 从Hbase中获取主题的相关字段
    */
  def getThemefeature(table: Table, rowkey: String, features: mutable.Buffer[String], ifNot: mutable.Buffer[String]) = {
    val getList = for (qualifier <- features)
      yield new Get(Bytes.toBytes(rowkey)).addColumn(Bytes.toBytes("a"), Bytes.toBytes(qualifier))
    import scala.collection.JavaConversions._
    val resList = for (result <- table.get(getList))
      yield result.rawCells().map(f => ((Bytes.toString(f.getQualifier), Bytes.toString(f.getValue))))
    val resMap = resList.flatten.toMap
    for (index <- 0 until features.size) {
      ifNot(index) = resMap.getOrElse(features(index), ifNot(index))
    }
    ifNot
  }

  /**
    * 从Hbase中获取用户的主题下载记录
    */
  def getThemeSet(table: Table, rowkey: String, qualifier: String) = {
    if (rowkey != null && rowkey.length > 0) {
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes("a"), Bytes.toBytes(qualifier))
      var resSet = mutable.Set[(String, String, String, String)]()
      if (table.exists(get)) {
        val hasSet = table.get(get).getValue(Bytes.toBytes("a"), Bytes.toBytes(qualifier))
        resSet = byteArrayToObj(hasSet).asInstanceOf[mutable.Set[(String, String, String, String)]]
      }
      resSet
    } else {
      mutable.Set[(String, String, String, String)]()
    }
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
    * 增量更新本次用户主题下载记录至Hbase
    */
  def mergeHbaseData(currSet: mutable.Set[(String, String, String, String)], table: Table, rowkey: String, family: String, qualifier: String) = {
    if (rowkey.nonEmpty && rowkey.length > 0) {
      val getHistorySet = new Get(Bytes.toBytes(rowkey))
      getHistorySet.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))
      val mergedSet = if (!table.exists(getHistorySet))
        currSet
      else {
        val hasSet = table.get(getHistorySet).getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier))
        val historySet = byteArrayToObj(hasSet).asInstanceOf[mutable.Set[(String, String, String, String)]]
        currSet ++= historySet
        currSet
      }
      println(s"-------------$rowkey------$qualifier----------")
      println(mergedSet.mkString(","))
      val putMergedSet = new Put(Bytes.toBytes(rowkey))
      putMergedSet.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), objToByteArray(mergedSet))
      table.put(putMergedSet)
    }

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
