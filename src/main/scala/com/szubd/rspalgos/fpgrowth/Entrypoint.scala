package com.szubd.rspalgos.fpgrowth

import org.apache.spark.rdd.RDD
import org.apache.spark.rsp.RspRDD
import org.apache.spark.sql.RspContext._
import org.apache.spark.sql.{Row, RspDataset, SparkSession}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import com.szubd.rspalgos.scadistfim.ScaDistFim
import scala.util.Random

object Entrypoint {

  def onArgs(spark: SparkSession, args: Array[String]): Unit = {
    printf("onArgs: %s\n", args.reduce((a, b) => a + " " + b))
    args(0) match {
      case "logov" => logoShuffleVote(spark, args(1), args(2).toDouble, args(3).toDouble, args.slice(4, args.length).map(_.toInt))
      case "logob" => logoShuffleBroadCast(spark, args(1), args(2).toDouble, args.slice(3, args.length).map(_.toInt))
      case "spark" => sparkShuffle(spark, args(1), args(2).toDouble, args.slice(3, args.length).map(_.toInt))
      case _ => printf("Unknown type: %s\n", args(0))
    }
  }

  def logoShuffleVote(spark: SparkSession,
                      sourceFile: String,
                      minsup:Double,
                      vote:Double,
                      blockSizes: Array[Int]): Unit = {

    val rdf: RspDataset[Row] = spark.rspRead.parquet(sourceFile)
    val s = sourceFile.split("/").last
    val ss = s.split("_")(0)
    var jobs: Array[(Int, Array[Int])] = null
    val osize = rdf.rdd.getNumPartitions
    val partitionsList = List.range(0, osize)
    if (blockSizes.length > 0) {
      jobs = blockSizes.map(s => (s, Random.shuffle(partitionsList).toArray))
    } else {
      jobs = Array((0, Array()))
    }
    for ((sizex, partitions) <- jobs) {
      val size = sizex
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      val partitionCount = size
      val trainRdd = ScaDistFim.etl(rdf.getSubDataset(osize))
      val value1 = trainRdd.repartition(size)
      val l: Long = value1.count()
      val startTimeBase = System.nanoTime
      SmileFPGrowth.runv(spark,value1,vote,(l * minsup / size).toInt, "/user/luokaijing/FPG_Result_2/Logo/"+ss+"_Size_"+size.toString+"_blocks"+"_Support_"+minsup.toString,startTimeBase)
    }
  }

  def logoShuffleVote_txt(spark: SparkSession,
                      sourceFile: String,
                      minsup:Double,
                      vote:Double,
                      blockSizes: Array[Int]): Unit = {

    val rdf: RspDataset[Row] = spark.rspRead.text(sourceFile)
    var jobs: Array[(Int, Array[Int])] = null
    val partitionsList = List.range(0, rdf.rdd.getNumPartitions)
    if (blockSizes.length > 0) {
      jobs = blockSizes.map(s => (s, Random.shuffle(partitionsList).toArray))
    } else {
      jobs = Array((0, Array()))
    }
    for ((sizex, partitions) <- jobs) {
      val size = Math.ceil(sizex * 0.05).toInt
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      var partitionCount = size
      val trainRdd: RspRDD[Row] = rdf.rdd.getSubPartitions(partitions.slice(0, partitionCount))
      val l: Long = trainRdd.count()
      val value: RDD[String] = trainRdd.map((f: Row) => f.mkString(" "))
      SmileFPGrowth.runv_txt(value, vote, (l * minsup / size).toInt, "/user/luokaijing/FPG_Exps_Big/LogoV3/size_" + sizex * 100 + "_" + minsup)
    }
  }


  def logoShuffleBroadCast(spark: SparkSession,
                           sourceFile: String,
                           minsup:Double,
                           blockSizes: Array[Int]): Unit = {
    for (size <- blockSizes) {
      val block = (size / 5 * 10).toInt
      val sampleBlock = Math.ceil(block * 0.05).toInt
      val trainRdd = spark.rspRead.text("Items_" + size + "W_" + block + "_5K_RSP.txt")
      val partitionsList = List.range(0, trainRdd.rdd.getNumPartitions)
      val array: Array[Int] = Random.shuffle(partitionsList).toArray
      val value1: RspRDD[Row] = trainRdd.rdd.getSubPartitions(array.slice(0, sampleBlock))
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      val value: RDD[String] = value1.map((f: Row) => f.mkString(" "))
      println(value.first())
      println(trainName)
      val l: Long = value.count()
      SmileFPGrowth.runb(value, l, minsup, (l * minsup / sampleBlock).toInt, "/user/caimeng/fpg/logob/size_" + size + "_minsup_0.15_new_50_")
    }
  }

  def sparkShuffle(spark: SparkSession,
                   sourceFile: String,
                   minsup:Double,
                   blockSizes: Array[Int]): Unit = {

    val startTimeBase = System.nanoTime
    val rdf: RspDataset[Row] = spark.rspRead.parquet(sourceFile)
    val s = sourceFile.split("/").last
    val ss = s.split("_")(0)
    var jobs: Array[(Int, Array[Int])] = null
    val osize = rdf.rdd.getNumPartitions
    val partitionsList = List.range(0, osize)

    if (blockSizes.length > 0){
      jobs = blockSizes.map(s => (s, Random.shuffle(partitionsList).toArray))
    }else{
      jobs = Array((0, Array()))
    }
    for ((size, partitions) <- jobs) {
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      val partitionCount = size
      val trainRdd: RspRDD[Row] = rdf.rdd.getSubPartitions(osize)
      val value: RDD[String] = trainRdd.map((f: Row) => f.get(0).asInstanceOf[mutable.WrappedArray[Int]].toArray.mkString(" "))
      val value1 = value.repartition(size)
      value1.count()
      SparkFPGrowth.run(spark,value1, size, "/user/luokaijing/FPG_Result_2/Spark/"+ss+"_Size_"+partitionCount.toString+"_blocks"+"_Support_"+minsup.toString,minsup)
    }
  }
}