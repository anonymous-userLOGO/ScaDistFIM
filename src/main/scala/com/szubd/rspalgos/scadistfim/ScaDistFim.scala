package com.szubd.rspalgos.scadistfim

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import smile.association.{ItemSet, fpgrowth}
import scala.collection.JavaConverters._

import scala.collection.mutable

object ScaDistFim {

  def etl(df: DataFrame): RDD[Array[Int]] = {
    df.rdd.map(_.get(0).asInstanceOf[mutable.WrappedArray[Int]].toArray)
  }

  def run(rdd: RDD[Array[Int]], minSupport: Double): RDD[(Array[Int], Int)] = {
    var results = rdd.mapPartitions(
      it => {
        var arr = it.toArray
        fpgrowth(
          (minSupport * arr.length).toInt,
          arr
      ).iterator().asScala.map(itemSet => (itemSet.items, itemSet.support))
      }
    )
    results.groupBy(
      f => f._1.sorted.toList
    ).map(
      group => (group._1.toArray, group._2.map(itemSet => itemSet._2).sum)
    )
  }
}
