package com.szubd.rspalgos.classification

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


/**
 *
 * @tparam M: TrainModel class
 */
trait LogoClassifier[M]{
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
   * @return trainData: RDD[ (Array[Int], Array[ Array[Double] ]) ]
   */
  def etl(rdd: RDD[Row]): RDD[(LABEL, FEATURE)]

  /**
   *
   * @param sample: 用于训练的数据类型
   * @return (model: M，trainTimeSeconds: Double)
   */
  def trainer(sample: (LABEL, FEATURE)): (M, Double)

  /**
   *
   * @param prediction: 模型预测值
   * @param label: 原始标签
   * @return accuracy: Double
   */
  def estimator(prediction: LABEL, label: LABEL): Double

  /**
   *
   * @param model: M, 模型
   * @param features: FEATURE, 预测数据集
   * @return prediction: LABEL，预测结果
   */
  def predictor(model: M, features: FEATURE): LABEL
}
