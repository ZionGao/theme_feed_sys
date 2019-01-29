package com.moxiu.bigdata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partitioner

class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
  val fraction = 1 max numFilesPerRegion min 32

  override def numPartitions: Int = splits.length * fraction

  override def getPartition(key: Any): Int = {
    def bytes(n: Any) = n match {
      case s: String => Bytes.toBytes(s)
      case s: Long => Bytes.toBytes(s)
      case s: Int => Bytes.toBytes(s)
      case s: Array[Byte] => s
    }

    val h = (key.hashCode() & Int.MaxValue) % fraction
    for (i <- 1 until splits.length)
      if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

    (splits.length - 1) * fraction + h
  }

}
