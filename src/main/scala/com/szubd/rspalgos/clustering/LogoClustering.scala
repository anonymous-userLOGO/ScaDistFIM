package com.szubd.rspalgos.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait LogoClustering[M] {

  /**
   * 标签数据类型
   */
  type LABEL = Array[Int]

  /**
   * 特征数据类型
   */
  type FEATURE = Array[Array[Double]]

  /**
   *
   * @param rdd: 原始数据RDD
   * @return trainData: RDD[ Array[ Array[Double] ] ]
   */
  def etl(rdd: RDD[Row]): RDD[(LABEL, FEATURE)]

  /**
   *
   * @param sample: 用于训练的数据类型
   * @return (model: M，trainTimeSeconds: Double)
   */
  def trainer(sample: FEATURE): (M, Double)

  /**
   * 从训练模型中取样用于2次聚类
   * @param model
   * @return
   */
  def resample(model: M): FEATURE

  /**
   * 纯度，类似分类的accuracy，
   * @param model
   * @param test
   * @return
   */
  def purity(model: M, test: (LABEL, FEATURE)): Double

}
