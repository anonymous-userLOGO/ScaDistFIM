package com.szubd.rspalgos.clustering

import com.szubd.rspalgos.clustering.SparkBisectingKMeans
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.RspContext._
import spire.ClassTag

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{Map => MutablMap}
import scala.util.Random

object Entrypoint {
  /**
   *
   * @param spark
   * @param args: Array[String], spark|logo commands...
   */
  def onArgs(spark: SparkSession, args: Array[String]): Unit = {
    args(0) match {
      case "spark" => onFitSpark(spark, args)
      case "logo" => onFitLogo(spark, args)
      case "sparkShuffle" => sparkShuffle(
        spark,
        args(1), args(2), args(3),
        args(4).toDouble, args(5).toDouble,
        args.slice(6, args.length).map(_.toInt)
      )
      case "logoShuffle" => logoShuffle(
        spark,
        args(1), args(2), args(3),
        args(4).toDouble, args(5).toInt,
        args(6).toDouble,
        args.slice(7, args.length).map(_.toInt)
      )
      //      case "logof" => onFitLogo(spark, args, false)
      case _ => printf("Unknown type: %s\n", args(0))
    }
  }

  /**
   *
   * @param spark
   * @param args: Array[String], spark algorithm dataFile partitionFile fraction sizes...
   *  algorithm: kmeans
   *  dataFile: 实验数据文件
   *  partitionFile: 分块记录文件，主要记录对应文件不同大小的实验需要取那些分块
   *  centersFile: 记录样本分类中心点的文件中心点的文件
   *  sizes: 需要运算的数据大小，单位GB，支持50:50:500，可接受多参数输入例如: 50 100 150
   *
   *  sample: clt spark kmeans rspclf50_8000.parquet SubPartitionIds8000.parquet CenterClf.parquet 50 100 150 200 250 300 350 400 450 500
   */
  def onFitSpark(spark: SparkSession, args: Array[String]): Unit = {
    val algo = args(1)
    val sourceFile = args(2)
    val partitionFile = args(3)
    val centersFile = args(4)
    val frac = args(5).toDouble
    val sizes = args.slice(6, args.length).map(_.toInt)

    fitSpark(spark, algo, sourceFile, partitionFile, centersFile, frac, sizes)

  }

  /**
   *
   * @param spark
   * @param args: Array[String], spark algorithm dataFile partitionFile fraction sizes...
   *  algorithm: kmeans
   *  dataFile: 实验数据文件
   *  partitionFile: 分块记录文件，主要记录对应文件不同大小的实验需要取那些分块
   *  centersFile: 记录样本分类中心点的文件中心点的文件
   *  subs: rsp计算所用的数据比列， 0 ~ 1
   *  sizes: 需要运算的数据大小，单位GB，支持50:50:500，可接受多参数输入例如: 50 100 150
   *  sample: clt logo kmeans rspclf50_8000.parquet SubPartitionIds8000.parquet CenterClf.parquet 50 100 150 200 250 300 350 400 450 500
   */
  def onFitLogo(spark: SparkSession, args: Array[String]): Unit = {
    val algo = args(1)
    val sourceFile = args(2)
    val partitionFile = args(3)
    val centersFile = args(4)
    val subs = args(5).toDouble
    val tests = args(6).toInt
    val sizes = args.slice(7, args.length).map(_.toInt)

    fitLogo(spark, algo, sourceFile, partitionFile, centersFile, subs, tests, sizes)

  }

  def readCenters(spark: SparkSession, centersFile: String): Map[String, Array[Array[Double]]] = {
    val df = spark.read.parquet(centersFile)
    df.rdd.map(r => (
      r.getString(0),
      r.get(1).asInstanceOf[WrappedArray[DenseVector]].toArray.map(_.toArray)
    )).collect().toMap
  }

  def fitSpark(spark: SparkSession,
               algo: String,
               sourceFile: String,
               partitionFile: String,
               centersFile: String,
               frac: Double = 0.9,
               sizes: Array[Int]): Unit = {

    printf("fitAlgo: algorithm = %s\n", algo)
    printf("fitAlgo: sourceFile = %s\n", sourceFile)
    printf("fitAlgo: partitionFile = %s\n", partitionFile)

    var rdf = spark.rspRead.parquet(sourceFile)
    var pdf = spark.read.parquet(partitionFile)
    val centerMap = readCenters(spark, centersFile)
    val centers = centerMap(sourceFile)
    var parts = pdf.collect().map(
      r => (r.getInt(0), r.get(1).asInstanceOf[WrappedArray[Int]].toArray)
    )
    var jobs: Array[(Int, Array[Int])] = null
    if (sizes.size > 0) {
      printf("fit algo: sizes = %s\n", sizes.map(_.toString).reduce(_ + ", " + _))
      var sizeMapper = parts.toMap

      //      jobs = sizes.filter(s => sizeMapper.contains(s)).map(s => (s, sizeMapper(s)))
      jobs = sizes.map(s => (s, sizeMapper.getOrElse(s, Array.range(0, s))))

    } else {

      jobs = Array((0, Array[Int]()))
    }

    var df: DataFrame = null

    for ((size, partitions) <- jobs) {
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      var modelName = "models/spark_%s_%s_%d_%s.ml".format(algo, sourceFile, size, beginTime)
      //      var partitions = sizeMapper(size)
      //      var rdd = getSubRdd(rdf.rdd, partitions)

      if(partitions.length > 0){
        df = spark.createDataFrame(rdf.rdd.getSubPartitions(partitions), rdf.schema)
      }else{
        df = rdf
      }
      var Array(train, test) = df.randomSplit(Array(frac, 1-frac))

      algo match {
        case "kmeans" => runSpark(trainName, modelName, train, test, centers, SparkKMeans.SparkKMeansClustering)
        case "bisectingkmeans" => runSpark(trainName, modelName, train, test, centers, SparkBisectingKMeans.SparkBisectingKMeansClustering)
      }
    }
  }

  /**
   *
   * @param spark
   * @param algo 算法：kmeans|bisectingkmeans
   * @param sourceFile 数据原文件
   * @param centersFile 中心点文件
   * @param frac 训练集比例
   * @param partitionsUnit 单位数据块数, 计算取块数 = (size * partitionsUnit).toInt()
   * @param sizes Array[Int], 训练数据集大小
   */
  def sparkShuffle(spark: SparkSession,
                   algo: String,
                   sourceFile: String,
                   centersFile: String,
                   frac: Double = 0.9,
                   partitionsUnit: Double,
                   sizes: Array[Int]): Unit = {

    printf("fitAlgo: algorithm = %s\n", algo)
    printf("fitAlgo: sourceFile = %s\n", sourceFile)

    var rdf = spark.rspRead.parquet(sourceFile)
    var fileList = centersFile.split(":")
    val centerMap = readCenters(spark, fileList(0))
    val key: String = {
      if(fileList.length > 1) {
        fileList(1)
      }else{
        sourceFile
      }
    }
    val centers = centerMap(key)

    var jobs: Array[(Int, Array[Int])] = null
    val partitionsList = List.range(0, rdf.rdd.getNumPartitions)
    if (sizes.length > 0){
      jobs = sizes.map(s => (s, Random.shuffle(partitionsList).toArray))
    }else{
      jobs = Array((0, Array()))
    }

    var df: DataFrame = null
    var train: DataFrame = null
    var test: DataFrame = null

    for ((size, partitions) <- jobs) {
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      var modelName = "models/spark_%s_%s_%d_%s.ml".format(algo, sourceFile, size, beginTime)
      //      var partitions = sizeMapper(size)
      //      var rdd = getSubRdd(rdf.rdd, partitions)

      if(size > 0){
        var partitionCount = (size * partitionsUnit).toInt
        var samplePartitions = partitions.slice(0, partitionCount)
        printf("partitions=%s\n", samplePartitions.toList)
        df = spark.createDataFrame(rdf.rdd.getSubPartitions(samplePartitions), rdf.schema)
      }else{
        df = rdf
      }

      if (frac > 0) {
        var splitDF = df.randomSplit(Array(frac, 1-frac))
        train = splitDF(0)
        test = splitDF(1)
      }else{
        train = df.randomSplit(Array(-frac))(0)
        test = null
      }

      algo match {
        case "kmeans" => runSpark(trainName, modelName, train, test, centers, SparkKMeans.SparkKMeansClustering)
        case "bisectingkmeans" => runSpark(trainName, modelName, train, test, centers, SparkBisectingKMeans.SparkBisectingKMeansClustering)
      }

    }
  }

  def runSpark(trainName: String, modelName: String,
               train: DataFrame, test: DataFrame, centers: Array[Array[Double]],
               function: (DataFrame, DataFrame) => ClusteringResult): Unit = {
    printf("%s start\n", trainName)
    var result = function(train, test)
    printf("%s finished\n", trainName)
    printf("Time spend: %f\n", result.getDuration())
    val predictCenters = result.getCenters()
    val distance = minDistance(centers, predictCenters)
    printf("%s Distance: %f\n", trainName, distance)
    printf("%s Purity: %f\n", trainName, result.getPurity())

    centers.foreach(showArray)
    predictCenters.foreach(showArray)
    result.getModel().write.save(modelName)
  }

  def fitLogo(spark: SparkSession,
              algo: String,
              sourceFile: String,
              partitionFile: String,
              centersFile: String,
              subs: Double,
              tests: Int,
              sizes: Array[Int]): Unit = {

    printf("fitLogo: algorithm = %s\n", algo)
    printf("fitLogo: sourceFile = %s\n", sourceFile)
    printf("fitLogo: partitionFile = %s\n", partitionFile)

    var rdf = spark.rspRead.parquet(sourceFile)
    var pdf = spark.read.parquet(partitionFile)
    val centerMap = readCenters(spark, centersFile)
    val centers = centerMap(sourceFile)
    var parts = pdf.collect().map(
      r => (r.getInt(0), r.get(1).asInstanceOf[WrappedArray[Int]].toArray)
    )
    var jobs: Array[(Int, Array[Int])] = null
    if (sizes.size > 0) {
      printf("fit algo: sizes = %s\n", sizes.map(_.toString).reduce(_ + ", " + _))
      var sizeMapper = parts.toMap

      //      jobs = sizes.filter(s => sizeMapper.contains(s)).map(s => (s, sizeMapper(s)))
      jobs = sizes.map(s => (s, sizeMapper.getOrElse(s, Array.range(0, s+tests))))

    } else {

      jobs = Array((0, Array[Int]()))
    }

    var df: DataFrame = null
    var testDf: DataFrame = null

    for ((size, partitions) <- jobs) {
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      var modelName = "models/logo_%s_%s_%d_%s.ml".format(algo, sourceFile, size, beginTime)
      //      var partitions = sizeMapper(size)
      //      var rdd = getSubRdd(rdf.rdd, partitions)
      var trainParts = (partitions.length * subs).toInt
      if (trainParts < 1) {
        trainParts = 1
      }
      if(partitions.length > 0){
        df = spark.createDataFrame(rdf.rdd.getSubPartitions(partitions.slice(0, trainParts)), rdf.schema)
        testDf = spark.createDataFrame(rdf.rdd.getSubPartitions(partitions.slice(trainParts, trainParts+tests)), rdf.schema)
      }else{
        df = rdf
        testDf = spark.createDataFrame(rdf.rdd.getSubPartitions(tests), rdf.schema)
      }

      algo match {
        case "kmeans" => runLogo(trainName, modelName, df, testDf, centers, SmileKMeans)
        case "bisectingkmeans" => runLogo(trainName, modelName, df, testDf, centers, SmileBisectingKMeans)
      }
    }
  }

  /**
   *
   * @param spark
   * @param algo 算法：kmeans|bisectingkmeans
   * @param sourceFile 数据原文件
   * @param centersFile 中心点文件
   * @param subs rsp取块比例
   * @param tests 测试集块数，0表示跳过测试集
   * @param partitionsUnit 单位数据块数, 计算取块数 = (size * partitionsUnit).toInt()
   * @param sizes Array[Int], 训练数据集大小
   */
  def logoShuffle(spark: SparkSession,
                  algo: String,
                  sourceFile: String,
                  centersFile: String,
                  subs: Double,
                  tests: Int,
                  partitionsUnit: Double,
                  sizes: Array[Int]): Unit = {

    printf("fitLogo: algorithm = %s\n", algo)
    printf("fitLogo: sourceFile = %s\n", sourceFile)

    var rdf = spark.rspRead.parquet(sourceFile)
    var fileList = centersFile.split(":")
    val centerMap = readCenters(spark, fileList(0))
    val key: String = {
      if(fileList.length > 1) {
        fileList(1)
      }else{
        sourceFile
      }
    }
    val centers = centerMap(key)

    var jobs: Array[(Int, Array[Int])] = null
    val partitionsList = List.range(0, rdf.rdd.getNumPartitions)
    if (sizes.length > 0){
      jobs = sizes.map(s => (s, Random.shuffle(partitionsList).toArray))
    }else{
      jobs = Array((0, Array()))
    }

    var df: DataFrame = null
    var testDf: DataFrame = null

    for ((size, partitions) <- jobs) {
      var trainName = "train(size=%d)".format(size)
      var beginTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd-HHmmss"))
      var modelName = "models/logo_%s_%s_%d_%s.ml".format(algo, sourceFile, size, beginTime)
      //      var partitions = sizeMapper(size)
      //      var rdd = getSubRdd(rdf.rdd, partitions)
      var trainParts = (size * partitionsUnit * subs).toInt
      if (trainParts < 1) {
        trainParts = 1
      }
      df = spark.createDataFrame(rdf.rdd.getSubPartitions(partitions.slice(0, trainParts)), rdf.schema)
      if (tests > 0 ){
        testDf = spark.createDataFrame(rdf.rdd.getSubPartitions(partitions.slice(trainParts, trainParts+tests)), rdf.schema)
      } else {
        testDf = null
      }

      algo match {
        case "kmeans" => runLogo(trainName, modelName, df, testDf, centers, SmileKMeans)
        case "bisectingkmeans" => runLogo(trainName, modelName, df, testDf, centers, SmileBisectingKMeans)
      }
    }
  }

  def runLogo[M: ClassTag](trainName: String, modelName: String,
                           df: DataFrame, test: DataFrame, centers: Array[Array[Double]],
                           lc: LogoClustering[M]): Unit = {
    printf("%s start\n", trainName)
    val begin = System.nanoTime()
    val rdd = lc.etl(df.rdd)
    val modelsRDD = rdd.map(r => {
      var (m, d) = lc.trainer(r._2)
      (lc.resample(m), d)
    })
    val trainEnd = System.nanoTime()
    printf("Train time: %f\n", (trainEnd - begin) * 1e-9)

    val flatRDD = modelsRDD
      .flatMap(r=>r._1)
    val groupedCenters = flatRDD.collect()

    val flatEnd = System.nanoTime()
    printf("Flat time: %f\n", (flatEnd - trainEnd) * 1e-9)
    printf("Flat count: %d\n", groupedCenters.length)

    var (model, _) = lc.trainer(groupedCenters)
    val predictCenters = lc.resample(model)
    val duration = System.nanoTime() - begin
    printf("%s finished\n", trainName)
    printf("Time spend: %f\n", duration * 1e-9)

    val distance = minDistance(centers, predictCenters)
    printf("%s Distance: %f\n", trainName, distance)
    if (test == null){
      printf("%s Purity: %f\n", trainName, 1.0)
      return
    }

    var purity = lc.etl(test.rdd).map(item => lc.purity(model, item)).mean()
    printf("%s Purity: %f\n", trainName, purity)

    centers.foreach(showArray)
    predictCenters.foreach(showArray)
    //    model.write.save(modelName)
  }


  def minDistance(real: Array[Array[Double]], predict: Array[Array[Double]]): Double = {
    real.map(
      rc => predict.map(pc => pc.zip(rc).map(item => math.pow(item._1-item._2, 2)).sum).min
    ).sum
  }

  def showArray(vector: Array[Double]): Unit = {
    println((new DenseVector(vector)).toString)
  }

}
