package com.szubd.rspalgos.fpgrowth
import java.util
import org.apache.spark.rdd.RDD
import smile.association.{ItemSet, fpgrowth}
import java.util.stream.{Collectors, Stream}
import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.{Row, RspDataset, SparkSession}


object SmileFPGrowth {

  def etl(data: RDD[String]): RDD[Array[Array[Int]]] = {
    data.map(_.split(" ").map(_.toInt)).glom()
  }

  def runv(session: SparkSession,data: RDD[Array[Int]], vote: Double, minsup: Int, path: String, time: Long): Unit = {
    val startTimeBase = time
    val partitionCount: Int = data.getNumPartitions
    val transaction: RDD[Array[Int]] = data
    transaction.count()
    val value: RDD[Stream[ItemSet]] = transaction.mapPartitions(arr => {
      val array: Array[Array[Int]] = arr.toArray
      val partitionRes: Stream[ItemSet] = fpgrowth(minsup, array)
      Iterator.single(partitionRes)
    })
    val value2: RDD[ItemSet] = value.mapPartitions((stream: Iterator[Stream[ItemSet]]) => {
      val elem: Stream[ItemSet] = stream.next()
      val buf: mutable.Buffer[ItemSet] = elem.collect(Collectors.toList[ItemSet]).asScala
      buf.iterator
    })
    val value1: RDD[(String, Int)] = value2
      .filter(item => item.items.length > 1)
      .map((item: ItemSet) => (item.items.toList.sorted.mkString("{", ",", "}"), item.support))
    val map: Map[String, Int] = value1.map(item => (item._1, 1))
      .reduceByKey(_ + _)
      .filter(f => f._2 >= partitionCount * vote)
      .collect()
      .toMap

    import session.implicits._
    val value3: RDD[(String, Int)] = value1
      .filter(f => map.contains(f._1))
      .combineByKey(v => v, (t: Int, v: Int) => t + v, (t: Int, v: Int) => t + v)
      .map(item => {
        (item._1, (item._2 / map(item._1)))
      })
    val xxx = value3.toDF("items", "support")
    val durationBase = (System.nanoTime - startTimeBase) * 0.000000001
    println(s"LOGO_RunV Time Spend: ${durationBase}s")
    xxx.write.mode(SaveMode.Overwrite).parquet(path)
  }

  def runv_txt(data: RDD[String], vote: Double, minsup: Int, path: String): Unit = {
    val partitionCount: Int = data.getNumPartitions;
    val transaction: RDD[Array[Int]] = data.map((_: String).split(" ").map(_.toInt))
    transaction.count()
    val startTime = System.nanoTime
    val value: RDD[Stream[ItemSet]] = transaction.mapPartitions(arr => {
      val array: Array[Array[Int]] = arr.toArray
      val partitionRes: Stream[ItemSet] = fpgrowth(minsup, array)
      Iterator.single(partitionRes)
    })
    val value2: RDD[ItemSet] = value.mapPartitions((stream: Iterator[Stream[ItemSet]]) => {
      val elem: Stream[ItemSet] = stream.next()
      val buf: mutable.Buffer[ItemSet] = elem.collect(Collectors.toList[ItemSet]).asScala
      buf.iterator
    })
    val value1: RDD[(String, Int)] = value2
      .filter(item => item.items.length > 1)
      .map((item: ItemSet) => (item.items.toList.sorted.mkString("{", ",", "}"), item.support))
      .cache()

    //
    val map: Map[String, Int] = value1.map(item => (item._1, 1))
      .reduceByKey(_ + _)
      .filter(f => f._2 >= partitionCount * vote)
      .collect()
      .toMap

    val value3: RDD[(String, Int)] = value1
      .filter(f => map.contains(f._1))
      .combineByKey(v => v, (t: Int, v: Int) => t + v, (t: Int, v: Int) => t + v)
      .map(item => {
        (item._1, (item._2 / map(item._1)))
      })

    value3.map(x => x._1 + ": " + x._2).repartition(1).saveAsTextFile(path)
    val duration = (System.nanoTime - startTime) * 0.000000001 //System.nanoTime为纳秒，转化为秒
    printf("3rd Time spend: %f\n", duration)
  }

  def runb(data: RDD[String], count: Long, elem: Double, ar: Int, path: String): Unit = {
    val partitionCount: Int = data.getNumPartitions;
    val transaction: RDD[Array[Int]] = data.map((_: String).split(" ").map(_.toInt)).cache()
    transaction.count()
    val startTime = System.nanoTime
    val value: RDD[Stream[ItemSet]] = transaction.mapPartitions(arr => {
      val array: Array[Array[Int]] = arr.toArray
      val partitionRes: Stream[ItemSet] = fpgrowth(ar, array)
      Iterator.single(partitionRes)
    })
    val value2: RDD[ItemSet] = value.mapPartitions((stream: Iterator[Stream[ItemSet]]) => {
      val elem: Stream[ItemSet] = stream.next()
      val buf: mutable.Buffer[ItemSet] = elem.collect(Collectors.toList[ItemSet]).asScala
      buf.iterator
    })
    val broadcastList2 = value2
      .filter((item: ItemSet) => item.items.length > 1)
      .map((item: ItemSet) => item.items.toList.sorted.mkString(","))
      .distinct()
      .collect()

    broadcastList2.foreach(println)
    val broadcastList: Array[List[Int]] = broadcastList2.map(_.split(",").map(_.toInt).toList)
    val value1: RDD[(String, Int)] = transaction.mapPartitions((arr: Iterator[Array[Int]]) => {
      val temp: Array[List[Int]] = util.Arrays.copyOf(broadcastList, broadcastList.length)
      val set: Array[Set[Int]] = arr.map(_.toSet).toArray
      val partitionRes: Array[(String, Int)] = temp.map(items => { //List[Int]
        var count = 0
        for (orginalData <- set) { //List[Int]
          var b = true
          for (item <- items if b) { //Int
            if (!orginalData.contains(item)) {
              b = false
            }
          }
          if (b) {
            count = count + 1
          }
        }
        (items.mkString("{", ",", "}"), count)
      })
      partitionRes.iterator
    })

    value1.reduceByKey(_ + _).map(x => (x._1, x._2 * 1.0 / count)).filter(_._2 >= elem).map(x => x._1 + ": " + x._2).repartition(1).saveAsTextFile(path)
    val duration = (System.nanoTime - startTime) * 0.000000001
    printf("Time spend: %f\n", duration)
  }
}