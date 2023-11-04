package com.szubd.rspalgos.classification

import com.szubd.rspalgos.classification.Entrypoint.{onFitSpark, votePrediction}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.rsp.RspRDD
import org.apache.spark.sql.RspContext._
import org.apache.spark.sql.{Row, RspDataset, SparkSession}
import smile.validation.metric.Accuracy
import spire.ClassTag

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random


object StartUpLogo {
  def onArgs(spark:SparkSession, args:Array[String]): Unit = {
    args(0) match {
      case "spark" => onFitSpark(spark, args)
      case "logo" => onFitLogo(spark, args)
      case "logoGeo" => onFitLogoGeoDistribution(spark, args)
      case _ => printf("Unknown type: %s\n", args(0))
    }
  }

  /**
   * spark-submit --class com.szubd.rspalgos.App \
  --name LOGO_10GB_10Part_2 \
	--master yarn \
  --deploy-mode cluster \
	--driver-memory 16g \
  --executor-memory 16g \
  --jars spark-rsp_2.11-2.4.0.jar \
   rsp-algos-1.0-SNAPSHOT-jar-with-dependencies.jar clf-simple logo DT /user/zhangyuming/cluster_10GToTal_10PartitionsPerField_1000Dim_04Scale_3Field_1_field.parquet 2 0 7
   */
  /**
   *
   * @param spark
   * @param args : Array[String], logo algorithm dataFile partitionFile subs tests predicts tails sizes...
   *  algorithm: DT|LR|RF
   *  sourceFile: 实验数据文件
   *  predicts: 模型预测数据集块数
   *  tails: 模型集成过程中的头尾筛选比例
   *  trainParts: 模型训练数据块数
   * @param useScore
   */
  def onFitLogo(spark: SparkSession, args: Array[String], useScore: Boolean=true): Unit = {
    val algo = args(1) // 使用的算法
    val sourceFile = args(2)
    val predicts = args(3).toInt // 测试使用的数据块数目
    val tails = args(4).toDouble // 模型掐头去尾比率
    val trainParts = args(5).toInt // 训练用到的块数的块数
    fitLogo(spark, algo, sourceFile, predicts, tails, trainParts)
  }

  /**
   * spark-submit --class com.szubd.rspalgos.App \
  --name LOGO_10GB_10Part_GEO \
	--master yarn \
  --deploy-mode cluster \
	--driver-memory 16g \
  --executor-memory 16g \
   rsp-algos-1.0-SNAPSHOT-jar-with-dependencies.jar clf-simple logoGeo "DT,DT,DT" \
  "/user/zhangyuming/cluster_8G_8Partition_1000Dim_04Scale.parquet, /user/zhangyuming/cluster_8G_8Partition_1000Dim_04Scale.parquet, /user/zhangyuming/cluster_8G_8Partition_1000Dim_04Scale.parquet" \
  0 3 7 "/user/zhangyuming/predict_cluster_2G_2Partitions_1000Dim_04Scale.parquet"
   */
  /**
   * algoList:List[String],
                             sourceFiles: List[String],
                             tails: Double,
                             DcNum:Int,
                             trainParts:Int,
                             predictFile:String,
                             useScore: Boolean=true
   * @param spark
   * @param args
   * @param useScore
   */
  def onFitLogoGeoDistribution(spark: SparkSession, args: Array[String], useScore: Boolean=true): Unit = {
    val algoList = args(1).split(",").map(_.trim).toList // 使用的算法
    println("algoList:" + algoList)
    val sourceFiles = args(2).split(",").map(_.trim).toList
    println("sourceFiles:" + sourceFiles)
    val tails = args(3).toDouble // 模型掐头去尾比率
    val DcNum = args(4).toInt
    val trainParts = args(5).toInt // 训练用到的块数的块数
    val predictFile = args(6)
    println("predictFile:" + predictFile)
    fitLogoGeoDistribution(spark, algoList, sourceFiles, tails, DcNum, trainParts, predictFile)
  }

  def fitLogo(spark: SparkSession,
              algo: String,
              sourceFile: String,
              predictParts: Int,
              tails: Double,
              trainParts: Int, useScore: Boolean=true): Unit = {
    var rdf: RspDataset[Row] = spark.rspRead.parquet(sourceFile)
    var numOfPartition = rdf.rdd.getNumPartitions

    var partitions = Random.shuffle(List.range(0, numOfPartition)).toArray


    var trainName = "train(size=%d blocks)".format(trainParts)
    var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))

    var predictPartsIndex = trainParts + predictParts

    var trainRdd = rdf.rdd.getSubPartitions(partitions.slice(0, trainParts))


    var predictRdd = rdf.rdd.getSubPartitions(partitions.slice(trainParts, predictPartsIndex))

    printf("%s\n", trainName)
    algo match {
      case "RF" => runLogo(trainName, trainRdd, predictRdd, RandomForestSmile, tails, useScore)
      case "DT" => runLogo(trainName, trainRdd, predictRdd, DecisionTreesSmile, tails, useScore)
      case "LR" => runLogo(trainName, trainRdd, predictRdd, LogisticRegressionSmile, tails, useScore)
      case "SVM" => {
        trainRdd = new RspRDD[Row](trainRdd.repartition(trainRdd.getNumPartitions*8).cache())
        runLogo(trainName, trainRdd, predictRdd, SupportVectorMachinesSmile, tails, useScore)
      }
    }
  }


  def fitLogoGeoDistribution(spark: SparkSession,
                             algoList:List[String],
                             sourceFiles: List[String],
                             tails: Double,
                             DcNum:Int,
                             trainParts:Int,
                             predictFile:String,
                             useScore: Boolean=true):Unit = {
    assert(sourceFiles.length == DcNum && algoList.length == DcNum)
    var predictDf = spark.rspRead.parquet(predictFile)
    var predictRdd: RspRDD[Row] = predictDf.rdd
    var predictSeq = scala.collection.mutable.Buffer[RDD[(Long, Array[Int])]]()
    for(i <- 0 until(DcNum)){
      var sourceFile = sourceFiles(i)
      println("DC_" +i +" sourceFile:" + sourceFile)
      var algo = algoList(i)
      var rdf = spark.rspRead.parquet(sourceFile)
      var numOfPartition = rdf.rdd.getNumPartitions
      println("numOfPartition:" + numOfPartition)
      var partitions = Random.shuffle(List.range(0, numOfPartition)).toArray
      var trainName = "DC_%d, train(size=%d blocks)".format(i,trainParts)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))

      var trainRdd: RspRDD[Row] = rdf.rdd.getSubPartitions(partitions.slice(0, trainParts))
//      var trainRdd = rdf.rdd

      printf("DC_%d, %s\n", i, trainName)

      val predictRDD = algo match {
        case "RF" => runLogo(trainName, trainRdd, predictRdd, RandomForestSmile, tails, useScore)
        case "DT" => runLogo(trainName, trainRdd, predictRdd, DecisionTreesSmile, tails, useScore)
        case "LR" => runLogo(trainName, trainRdd, predictRdd, LogisticRegressionSmile, tails, useScore)
        case "SVM" => {
          trainRdd = new RspRDD[Row](trainRdd.repartition(trainRdd.getNumPartitions*8).cache())
          runLogo(trainName, trainRdd, predictRdd, SupportVectorMachinesSmile, tails, useScore)
        }
      }
      predictSeq += predictRDD
    }

    val prediction = unionAllDcPredict(predictSeq)
    val predictWithIndex = etl(predictRdd).zipWithIndex()
    val predicts = predictWithIndex.map(item => (item._2, item._1, item._1._1.length))
    val sizeRDD: RDD[(Long, Int)] = predicts.map(item => (item._1, item._3))
    val indexedLabels: RDD[(Long, Array[Int])] = predictWithIndex.map(item => (item._2, item._1._1))

    val rspPredicts: RDD[(Long, Array[Int])] = prediction.join(sizeRDD).map(votePrediction)

    val rspAcc = rspPredicts.join(indexedLabels).map(
      item => (estimator(item._2._1, item._2._2), item._2._1.length)
    )

    // 总的准确率
    val acc = rspAcc.map(item => item._1 * item._2).sum / rspAcc.map(_._2).sum
    //    val result = rspAcc.collect()
    //    val acc = result.map(item => item._1 * item._2).sum / result.map(_._2).sum
    var trainName = "GeoDis, train(size=%d blocks)".format(trainParts)
    printf("%s Accuracy: %f\n", trainName, acc)
  }


  def runLogo[
    M: ClassTag // Model
  ](trainName: String, trainRdd: RDD[Row], predictRdd: RDD[Row],
    classifier: LogoClassifier[M],
    tail: Double, useScore: Boolean=true): RDD[(Long, Array[Int])]  = {
    val beginTime = System.nanoTime

    //    var modelRdd = classifier.etl(trainRdd).map(
    //      classifier.trainer
    //    ).zipWithIndex().map(item => (item._2, item._1._1, item._1._2))

    // glom 就是每个rdd的每个partition内都是一个数组的形式
    var modelRdd = classifier.etl(trainRdd).map( // 对于每个partition
      sample => {
        val trainSize = (sample._1.length * 0.9).toInt
        val trainSample = (sample._1.slice(0, trainSize), sample._2.slice(0, trainSize))
        val testSample = ((sample._1.slice(trainSize, sample._1.length), sample._2.slice(trainSize, sample._1.length)))// 剩下的给测试集
        val (model, duration) = classifier.trainer(trainSample)
        val accuracy = classifier.estimator(classifier.predictor(model, testSample._2), testSample._1) // ._2特征 ._1标签
        (model, duration, accuracy)
      }
    )

    //    modelRdd.count()
    var valuedModels: RDD[(M, Double)] = null

    // 看验证集的情况
    if ( useScore ) {
      val integrateStart = System.nanoTime
      val factors = modelRdd.map(_._3).collect().sorted
      val mcount = factors.length
      printf("Model count: %d\n", mcount)
      var tcount = (mcount * tail).toInt
      // 至少要掐头去尾
      if(tcount < 1) {
        tcount = 1
      }
      printf("Tail count: %d\n", tcount)
      //  掐头去尾   eg.[0, 50] -> [15, 35]
      val (minAcc, maxAcc) = (factors(tcount), factors(mcount-tcount-1))
      printf("Score range: (%f, %f)\n", minAcc, maxAcc)
      //      printf("Range count: %d\n", )
      //      printf("Time integrate: %f\n", (System.nanoTime - integrateStart) * (1e-9))
      valuedModels = modelRdd.filter(item => minAcc <= item._3 && item._3 <= maxAcc).map(item => (item._1, item._3))
    } else {
      valuedModels = modelRdd.map(item => (item._1, 1.0))
    }

    val endTime = System.nanoTime()
    printf("Time spend: %f\n", (endTime - beginTime) * (1e-9) )
    //
    if (predictRdd == null) {
      printf("%s Accuracy: %f\n", trainName, 1.0)
      return null
    }
    printf("ValuedModel count: %d\n", valuedModels.count())
    val predictWithIndex: RDD[((classifier.LABEL, classifier.FEATURE), Long)] = classifier.etl(predictRdd).zipWithIndex() // 将rdd索引加入
    val predicts = predictWithIndex.map(item => (item._2, item._1, item._1._1.length)) // 每个预测part有数据块id，数据(标签和特征)，数据大小
    // 根据partitionId 来进行分组，统一partitionId的预测结果放在一个组里
    val prediction: RDD[(Long, Iterable[(Long, classifier.LABEL)])] = valuedModels.cartesian(predicts).map(
      item => (item._2._1, classifier.predictor(item._1._1, item._2._2._2)) //item._2._1:id,item._1._1：model,item._2._2._2:特征
    ).groupBy(_._1) // 根据part_id排序
    val sizeRDD: RDD[(Long, Int)] = predicts.map(item => (item._1, item._3)) // id, size
    // 投票 prediction.join(sizeRDD) rdd((id,(pred, id_size)),...)
    // RDD[(Long, (Iterable[(Long, classifier.LABEL)], Int))]
    // 某个partitionId的预测结果[partitionId, [0,1,1,0,...]]
    val rspPredict: RDD[(Long, Array[Int])] = prediction.join(sizeRDD).map(votePrediction) // id, pred, size
    val indexedLabels: RDD[(Long, classifier.LABEL)] = predictWithIndex.map(item => (item._2, item._1._1)) // part_id, 标签
    val rspAcc = rspPredict.join(indexedLabels).map(
      item => (classifier.estimator(item._2._1, item._2._2), item._2._1.length)
    )

    // 总的准确率
    val acc = rspAcc.map(item => item._1 * item._2).sum / rspAcc.map(_._2).sum
    //    val result = rspAcc.collect()
    //    val acc = result.map(item => item._1 * item._2).sum / result.map(_._2).sum
    printf("%s Accuracy: %f\n", trainName, acc)

    rspPredict
  }


  def unionAllDcPredict(rddSeq:Seq[RDD[(Long, Array[Int])]]):RDD[(Long, Iterable[(Long, Array[Int])])] = {
    val unionRDD = rddSeq.reduce((rddA, rddB) => rddA.union(rddB))

    val mappedRDD = unionRDD.map{ case (key, array) => ((key, key), array)}
    val groupedRDD: RDD[((Long, Long), Iterable[Array[Int]])] = mappedRDD.groupByKey()


    val resultRDD: RDD[(Long, Iterable[(Long, Array[Int])])] = groupedRDD.map {
      case ((id, _), iterableArrays) =>
        (id, iterableArrays.map(array => (id, array)))
    }

    resultRDD
  }


  def etl(rdd: RDD[Row]): RDD[(Array[Int], Array[Array[Double]])] = {
    rdd.glom().map(
      // 对于每个分区
      f => (
        // 对于每个分区内的每个对[label, features]
        f.map(r=>r.getInt(0)), // array
        // 对于每个分区内的每个对[label, features]
        f.map(r=>r.get(1).asInstanceOf[DenseVector].toArray) // array[array]
      )
    )
  }

  def estimator(prediction: SupportVectorMachinesSmile.LABEL, label: SupportVectorMachinesSmile.LABEL): Double = {
    Accuracy.of(label, prediction)
  }
}
