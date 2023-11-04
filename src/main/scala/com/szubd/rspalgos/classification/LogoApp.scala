package com.szubd.rspalgos.classification
//

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object LogoApp {

  def run[M: ClassTag](df : org.apache.spark.sql.DataFrame, logoClassifier: LogoClassifier[M], frac: Double=0.9): RDD[M] = {
    //    格式转换
//    val rdd = logoClassifier.etl(df.rdd)
    //    分训练和预测
    val Array(
      trainRDD: RDD[(Array[Int], Array[Array[Double]])],
      testRDD: RDD[(Array[Int], Array[Array[Double]])]
    ) = df.rdd.randomSplit(Array(0.9, 0.1)).map(rdd => logoClassifier.etl(rdd.coalesce(1)))
    //    训练
    val resultRDD = trainRDD.map(logoClassifier.trainer)
    val modelRDD = resultRDD.map(_._1)

    //    预测
    val predictRDD = modelRDD.cartesian(testRDD).map(
      f=>logoClassifier.estimator(logoClassifier.predictor(f._1, f._2._2), f._2._1)
    )
    //    获取模型在每个训练集分块上的accuracy
    val accuracies = predictRDD.collect()
    printf("%s\n", accuracies.map(_.toString).reduce(_+", "+_))

//    val testData = testRDD.collect()
//    for (m <- models){
//
//      for ((l, f) <- testData) {
//        val start = System.nanoTime
//        logoClassifier.estimator(logoClassifier.predictor(m, f), l)
//        printf("Single estimate time: %f\n", (System.nanoTime - start) * (1e-9))
//      }
//    }

    modelRDD
//    accuracies.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RandomForest-Classification-Smile").setMaster("yarn")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    var frame: org.apache.spark.sql.DataFrame = spark.read.parquet("/user/caimeng/classification_50_2_0.54_5_64M.parquet")
    if(args.length == 1) {
      val s = args(0).toDouble
      frame = frame.sample(false, s)
    }
//    RandomForestSmile.trees = 40
//    RandomForestSmile.depth = 20
//    run(frame, RandomForestSmile)
//    run(frame, DecisionTreesSmile)
//    run(frame, LogisticRegressionSmile)
    val modelRDD = run(frame, SupportVectorMachinesSmile)

  }

}
