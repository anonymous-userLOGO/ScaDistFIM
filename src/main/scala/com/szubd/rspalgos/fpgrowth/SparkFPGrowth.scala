package com.szubd.rspalgos.fpgrowth

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkFPGrowth {
  def etl(data: RDD[String]): RDD[Array[String]] = {
    data.map(_.split(" "))
  }

  def run(session: SparkSession,data: RDD[String], partitions:Int, path:String, minsup:Double): Unit = {
    val startTime = System.nanoTime
    val data1: RDD[Array[String]] = etl(data).cache()
    val fpg = new FPGrowth().setMinSupport(minsup)
    val model = fpg.run(data1)
    val value: RDD[FPGrowth.FreqItemset[String]] = model.freqItemsets.filter(f => f.items.length > 1)
    value.count()
    import session.implicits._
    val value4 = value.map(f => (f.items.toList.sorted.mkString("{", ",", "}"),f.freq.toInt)).cache()
    val value4_df = value4.toDF("items", "support")
    value4_df.write.mode(SaveMode.Overwrite).parquet(path)
    val duration = (System.nanoTime - startTime) * 0.000000001
    println(s"Spark time spend ${duration} =========support:${minsup}=========")
  }
}