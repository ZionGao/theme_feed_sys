package com.moxiu.bigdata

import java.io.IOException
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.mapreduce.Job


object LoadDataService {
  val prop = new Properties()
  val inputStream = LoadDataService.getClass.getClassLoader.getResourceAsStream("settings.properties")
  //val inputStream = LoadDataService.getClass.getClassLoader.getResourceAsStream("s.properties")
  prop.load(inputStream)
  //用户设置
  System.setProperty("user.name", prop.getProperty("HADOOP_USER_NAME"))
  System.setProperty("HADOOP_USER_NAME", prop.getProperty("HADOOP_USER_NAME"))
  val profileListOutputPath = prop.getProperty("profileListOutputPath")
  val simUserListOutputPath = prop.getProperty("simUserListOutputPath")
  val numOfHotTheme = prop.getProperty("numOfHotTheme").toInt
  val listSizeOfSimRecommend = prop.getProperty("listSizeOfSimRecommend").toInt
  val numOfThemePerUser = prop.getProperty("numOfThemePerUser").toInt
  val finalFolder = prop.getProperty("finalFolder")
  val outputTable = prop.getProperty("outputTable")
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
    val conf = new SparkConf().setAppName("LoadFeedData")
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
    val shell = new FsShell(hadoopConf)
    val htable = new HTable(hbaseConfiguration, outputTable)

    // val date = "2019-01-23 00:00:00"
    // val option = Array("makeRecommendList")
    // val date = args(0)
    //val option = args(1).split(",")
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
    if (hdfs.exists(new Path(finalFolder))) {
      println("delete exits recallFolder tmp files") //清空临时文件目录
      hdfs.delete(new Path(finalFolder), true)
    }
    /**
      * ==============================================  清理路径  ==========================================================
      **/
    val resTheme = sqlContext.sql("select rank,id from reserve_theme_rank").rdd
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .sortBy(_._1)
      .map(_._2)
    val broadTheme = sc.broadcast(resTheme)
    val profileList = sc.textFile(profileListOutputPath + "/*")
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(arr => (arr(0), (arr(1), arr(2))))

    val simUserList = sc.textFile(simUserListOutputPath + "/*")
      .map { line =>
        val arr = line.split("\t")
        (arr(0), arr(1))
      }
    val outputData = profileList.fullOuterJoin(simUserList, new HFilePartitioner(hbaseConfiguration, htable.getStartKeys, 20))
      .mapPartitions { partition =>
        import scala.collection.JavaConverters._
        val kvs = new util.TreeSet[KeyValue](KeyValue.RAW_COMPARATOR)
        partition
          .flatMap { line =>
            val themeForSim = broadTheme.value.take(listSizeOfSimRecommend)
            val themeForProfile = broadTheme.value.drop(listSizeOfSimRecommend)
            val simList = line._2._2 match {
              case l: Some[String] => l.get
              case _ => themeForSim.mkString("\002")
            }
            val (proList, extendList) = line._2._1 match {
              case l: Some[(String, String)] => l.get
              case _ => (themeForProfile.take(numOfThemePerUser).mkString("\002"), themeForProfile.drop(numOfThemePerUser).take(numOfThemePerUser).mkString("\002"))
            }
            val pushingScheme = Array("simuser", "profile", "extend")
            val pushingValue = Array(simList, proList, extendList)
            val pushingList = for (one <- pushingScheme.zip(pushingValue)) yield new KeyValue(Bytes.toBytes(line._1), Bytes.toBytes("pushing"), Bytes.toBytes(one._1), Bytes.toBytes(one._2))
            val pushedScheme = Array("top", "single", "cp", "train", "topic", "simuser", "profile", "extend")
            val pushedList = for (one <- pushedScheme) yield new KeyValue(Bytes.toBytes(line._1), Bytes.toBytes("pushed"), Bytes.toBytes(one), Bytes.toBytes("0"))
            (pushingList ++ pushedList)
          }
          .foreach(one => kvs.add(one))
        kvs.iterator().asScala.map(one => (new ImmutableBytesWritable(one.getKey), one))
      }
      .saveAsNewAPIHadoopFile(finalFolder,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseConfiguration)

    try
      shell.run(Array[String]("-chmod", "-R", "777", finalFolder))
    catch {
      case e: Exception =>
        println("Couldnt change the file permissions ", e)
        throw new IOException(e)
    }
    val load = new LoadIncrementalHFiles(hbaseConfiguration)
    val table: Table = connector.getTable(TableName.valueOf(outputTable))
    try {
      //获取hbase表的region分布
      // val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
      val job = Job.getInstance(hbaseConfiguration)
      job.setJobName("DumpFile")
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)
      //开始导入
      load.doBulkLoad(new Path(finalFolder), table.asInstanceOf[HTable])
    } finally {
      table.close()
    }

    sc.stop()


  }
}
